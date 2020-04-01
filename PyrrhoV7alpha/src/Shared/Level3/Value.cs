using System;
using System.IO;
using System.Collections.Generic;
using System.Text;
using System.Globalization;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System.Xml;
using System.Net;
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
    /// </summary>
    internal class SqlValue : DBObject,IComparable
    {
        internal const long
            _From = -306, // long
            Info = -307, // ObInfo
            Left = -308, // SqlTable
            Right = -309, // SqlValue
            Sub = -310; // SqlValue
        public string name => (string)mem[Name]??"";
        internal SqlValue left => (SqlValue)mem[Left];
        internal SqlValue right => (SqlValue)mem[Right];
        internal SqlValue sub => (SqlValue)mem[Sub];
        internal long from => (long)(mem[_From]??-1L);
        internal ObInfo info => (ObInfo)mem[Info]??ObInfo.Any;
        internal virtual long target => defpos;
        public BTree<string, BTree<int, long>> methods =>
        (BTree<string, BTree<int, long>>)mem[ObInfo.Methods] ?? BTree<string, BTree<int, long>>.Empty;
        public SqlValue(long dp, string nm, Domain dt, BTree<long, object> m = null)
            : this(dp, nm, (m ?? BTree<long,object>.Empty) + (_Domain,dt))
        { }
        public SqlValue(long dp, string nm, BTree<long, object> m = null)
            : base(dp, _Info(dp,nm,m) + (Name, nm)) 
        { }
        protected SqlValue(long dp,BTree<long,object> m):base(dp,_Info(dp,null,m))
        { }
        static BTree<long,object> _Info(long dp,string nm,BTree<long,object> m)
        {
            m = m ?? BTree<long, object>.Empty;
            var a = (string)m[_Alias];
            nm = a ?? nm ?? "";
            var dm = (Domain)m[_Domain] ?? Domain.Content;;
            var oi = (ObInfo)m[Info];
            if (oi == null)
                oi = new ObInfo(dp, nm, dm);
            else if (oi.defpos == -1)
                oi = new ObInfo(dp, nm, dm, oi.columns);
            else if (nm != "" && oi.name == "")
                oi += (Name, nm);
            return m + (Info,oi);
        }
        public static SqlValue operator+(SqlValue s,(long,object)x)
        {
            return (SqlValue)s.New(s.mem + x);
        }
        internal override bool Calls(long defpos, Database db)
        {
            return left?.Calls(defpos, db)==true || right?.Calls(defpos,db)==true;
        }
        /// <summary>
        /// The Import transformer allows the the import of an SqlValue expression into a subquery
        /// or view. This means that identifiers/column names will refer to their meanings inside
        /// the subquery.
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="q">The inner query</param>
        /// <returns></returns>
        internal virtual SqlValue Import(Query q)
        {
            return this;
        }
        internal virtual SqlValue Reify(Context cx,ObInfo oi)
        {
            if (oi.columns[oi.map[name] ?? -1] is ObInfo ci)
                return new SqlTableCol(ci.defpos, name, oi.defpos,
                    (TableColumn)cx.db.objects[ci.defpos]);
            return this;
        }
        internal virtual SqlValue Reify(Context cx,Ident ic)
        {
            return this;
        }
        internal virtual SqlValue AddReg(Context cx,long rp,TRow key)
        {
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
        internal virtual SqlValue _ColsForRestView(long dp, Context cx, From gf, GroupSpecification gs,
            ref BTree<SqlValue, string> gfc, ref BTree<SqlValue, string> rem, ref BTree<string, bool?> reg,
            ref BTree<long, SqlValue> map)
        {
            throw new NotImplementedException();
        }
        internal virtual bool Grouped(GroupSpecification gs)
        {
            return gs?.Has(this) == true;
        }
        internal virtual (SqlValue,Query) Resolve(Context cx,Query q)
        {
            if (domain != Domain.Content)
                return (this,q);
            if (q is From fm && (name==q?.name || name == q?.alias))
            {
                var st = new SqlTable(defpos, name, fm);
                if (q is FromOldTable fo)
                    st = new SqlOldTable(defpos, name, fo);
                return ((SqlValue)cx.Replace(this, st),q);
            }
            if (cx.defs.Contains(name))
            {
                var (ob, _) = cx.defs[name];
                if (q.rowType==Selection.Star)
                    return (this,q);
                if (ob.kind != Sqlx.CONTENT && ob.defpos != defpos && ob is SqlValue sb)
                {
                    var nc = (SqlValue)cx.Replace(this,
                        new SqlValue(defpos, name, sb.info.domain,mem));
                    cx.Add(((Query)cx.obs[from]).AddMatchedPair(nc, sb));
                    q = q.AddMatchedPair(sb,nc);
                    return (nc,q);
                }
            }
            var rt = q?.rowType;
            if (name != "" && (rt?.map?.Contains(name)==true))
            {
                var i = rt.map[name].Value;
                var sv = rt[i];
                var ns = sv;
                if (sv is SqlCopy sc && alias !=null && alias != sc.name)
                    ns = ns + (Name, alias) + (Info,ns.info+(Name,alias));
                if (sv is SqlTableCol st)
                {
                    if (q is FromOldTable fo)
                        ns = new SqlOldRowCol(defpos, name, fo, st.tableCol);
                    else
                        ns = new SqlCopy(defpos, alias ?? name, 
                            st.info+(Name,alias??name),st.defpos, st.tableCol.defpos);
                }
                else if ((!(sv is SqlCopy)) && (!cx.rawCols) && sv.kind != Sqlx.CONTENT)
                    ns = new SqlCopy(defpos, alias ?? name, 
                        sv.info+(Name,alias??name), rt.defpos, sv.defpos);
                ns = ns.Reify(cx, new Ident(name,defpos));
                var nc = (SqlValue)cx.Replace(this,ns);
                q += (i, nc);
                return (nc,q);
            }
            return (this,q);
        }
        internal virtual Query AddMatches(Query f)
        {
            return f;
        }
        /// <summary>
        /// Get the position of a given expression, allowing for adapter functions
        /// </summary>
        /// <param name="sv">the expression</param>
        /// <returns>the position in the query</returns>
        internal virtual int ColFor(Context cx,Query q)
        {
            return q.rowType.map[name] ?? -1;
        }
        internal override TypedValue Eval(Context cx)
        {
            var t = cx.copy[defpos];
            if (t == null)
            {
                if (!cx.from.Contains(defpos))
                    return TNull.Value; // can happen if the source rowset is trivial
                var r = cx.data[cx.from[defpos]]
                    ?? throw new PEException("PE192");
                var c = cx.cursors[r.defpos]
                    ?? throw new PEException("PE191");
                return c[defpos] ?? TNull.Value;
            }
            for (var b = t.First(); b != null; b = b.Next())
            {
                var k = b.key();
                if (!cx.from.Contains(k))
                    continue;
                var rs = cx.data[cx.from[k]];
                if (rs != null)
                {
                    var cu = cx.cursors[rs.defpos]
                        ?? throw new PEException("PE193");
                    return cu[k];
                }
            }
            throw new PEException("PE195");
        }
        internal virtual BTree<long,SqlValue> Disjoin()
        {
            return new BTree<long,SqlValue>(defpos, this);
        }
        internal virtual bool Uses(long t)
        {
            return false;
        }
        internal override DBObject TableRef(Context cx, From f)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            if (domain == Domain.Content && f.mem.Contains(_Alias) && name == f.alias)
            {
                var r = new SqlTable(defpos, name, f);
                cx.done += (defpos,r);
                return r;
            }
            return base.TableRef(cx, f);
        }
        /// <summary>
        /// Used for Window Function evaluation.
        /// Called by GroupingBookmark (when not building) and SelectedCursor
        /// </summary>
        /// <param name="bmk"></param>
        internal virtual void OnRow(Cursor bmk)
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
        internal virtual bool _MatchExpr(Query q,SqlValue v)
        {
            return defpos==v.defpos;
        }
        internal bool MatchExpr(Query q,SqlValue v)
        {
            return q.MatchedPair(this, v)||_MatchExpr(q,v);
        }
        /// <summary>
        /// analysis stage conditions(): test to see if this predicate can be distributed.
        /// </summary>
        /// <param name="q">the query to test</param>
        /// <param name="ut">(for RestView) a usingTableType</param>
        /// <returns>true if the whole of thsi is provided by q and/or ut</returns>
        internal virtual bool IsFrom(Query q, bool ordered=false, Domain ut=null)
        {
            return false;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            return (defpos == so.defpos) ? sv : this;
        }
        internal virtual int? _PosIn(Transaction tr, Query q, bool ordered,out Ident sub)
        {
            sub = null;
            return null;
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
        internal virtual Selection Needs(Selection qn)
        {
            return (qn.map.Contains(name)) ? qn : qn + this;
        }
        internal int? PosIn(Transaction tr,Query q,bool ordered,out Ident sub)
        {
            var r = _PosIn(tr, q, ordered, out sub);
            if (!(q is JoinPart))
            for (var b = q.matching[this]?.First();r==null && b!=null;b=b.Next())
                r = b.key()._PosIn(tr, q, ordered, out sub);
            return r;
        }
        internal virtual bool isConstant
        {
            get { return false; }
        }
        /// <summary>
        /// Analysis stage Conditions: update join conditions
        /// </summary>
        /// <param name="tr">The connection</param>
        /// <param name="j">The joinpart</param>
        /// <param name="joinCond">Check this only contains simple left op right comparisons</param>
        /// <param name="where">See if where contains any suitable joincoditions and move them if so</param>
        internal virtual Query JoinCondition(Context cx, JoinPart j, ref BTree<long, SqlValue> joinCond, ref BTree<long, SqlValue> where)
        {
            j += (JoinPart.LeftOperand, j.left.AddCondition(cx,Query.Where, this));
            j += (JoinPart.RightOperand, j.right.AddCondition(cx,Query.Where,this));
            return j;
        }
        /// <summary>
        /// Analysis stage Conditions: Distribute conditions to joins, froms
        /// </summary>
        /// <param name="q"> Query</param>
        /// <param name="repl">Updated list of equality conditions for possible replacements</param>
        /// <param name="needed">Updated list of fields mentioned in conditions</param>
        internal virtual Query DistributeConditions(Context cx,Query q, BTree<SqlValue,SqlValue> repl)
        {
            return q;
        }
        internal virtual SqlValue PartsIn(Selection dt)
        {
            for (var b=dt.First();b!=null;b=b.Next())
                if (defpos==b.value().defpos)
                    return this;
            return null;
        }
        internal virtual void BuildRepl(Query lf,ref BTree<SqlValue,SqlValue> lr,ref BTree<SqlValue,SqlValue> rr)
        {
        }
        internal bool Matches(Context cx)
        {
            return (Eval(cx) is TypedValue tv) ? tv == TBool.True : true;
        }
        internal virtual bool HasAnd(SqlValue s)
        {
            return s == this;
        }
        internal virtual SqlValue Invert()
        {
            return new SqlValueExpr(defpos, Sqlx.NOT, this, null, Sqlx.NO);
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
        internal virtual RowSet RowSet(long dp,Context cx,ObInfo rt)
        {
            if (rt.Eval(cx) is TRow r)
                return new TrivialRowSet(dp,cx, r);
            cx.data += (dp, EmptyRowSet.Value);
            return EmptyRowSet.Value;
        }
        internal virtual Domain FindType(Domain dt)
        {
            var vt = domain;
            if (vt == null || vt.kind==Sqlx.CONTENT)
                return dt;
            if (!dt.CanTakeValueOf(vt))
                throw new DBException("22005U", dt.kind, vt.kind);
            if ((isConstant && vt.kind == Sqlx.INTEGER) || vt.kind==Sqlx.Null)
                return dt; // keep union options open
            return vt;
        }
        public virtual int CompareTo(object obj)
        {
            return (obj is SqlValue that)?defpos.CompareTo(that.defpos):1;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(Flag());
            sb.Append(base.ToString());
            if (mem.Contains(_From)) { sb.Append(" From:"); sb.Append(Uid(from)); }
            if (mem.Contains(_Alias)) { sb.Append(" Alias="); sb.Append(alias); }
            if (mem.Contains(Left)) { sb.Append(" Left:"); sb.Append(left); }
            if (mem.Contains(_Domain)) { sb.Append(" _Domain"); sb.Append(domain); }
            if (mem.Contains(Right)) { sb.Append(" Right:"); sb.Append(right); }
            if (mem.Contains(Sub)) { sb.Append(" Sub:"); sb.Append(sub); }
            return sb.ToString();
        }
        protected string Flag()
        {
            var dm = domain;
            switch (dm.kind)
            {
                case Sqlx.VALUE:
                case Sqlx.CONTENT:
                case Sqlx.UNION:
                    return dm.kind.ToString() + " ";
            }
            return " ";
        }
        /// <summary>
        /// Compute relevant equality pairings.
        /// Currently this is only for EQL joinConditions
        /// </summary>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal virtual void Eqs(Context cx,ref Adapters eqs)
        {
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
        internal virtual void _AddReqs(Transaction tr,Query gf,Domain ut, ref BTree<SqlValue,int> gfreqs,int i)
        {
            if (from==gf.defpos)
                gfreqs +=(this, i);
        }
        internal void AddReqs(Transaction tr, Query gf, Domain ut, ref BTree<SqlValue, int> gfreqs, int i)
        {
            for (var b = gfreqs.First(); b != null; b = b.Next())
                if (MatchExpr(gf, b.key()))
                    return;
            _AddReqs(tr, gf, ut, ref gfreqs, i);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlValue(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlValue(dp, mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = ((SqlValue)base.Relocate(wr)).Relocate(wr.Fix(defpos));
            var nf = info.Relocate(wr);
            if (nf != info)
                r += (Info, nf);
            var f = wr.Fix(from);
            if (f != from)
                r += (_From, f);
            var lf = left?.Relocate(wr);
            if (lf != left)
                r += (Left, lf);
            var rg = right?.Relocate(wr);
            if (rg != right)
                r += (Right, rg);
            var dt = domain.Relocate(wr);
            if (dt != domain)
                r += (_Domain, dt);
            var sb = sub?.Relocate(wr);
            if (sb != sub)
                r += (Sub, sb);
            // don't worry about TableRow
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = this;
            var lf = left?.Frame(cx);
            if (lf != left)
                r += (Left, lf);
            var rg = right?.Frame(cx);
            if (rg != right)
                r += (Right, rg);
            var dt = domain.Frame(cx);
            if (dt != domain)
                r += (_Domain, dt);
            var sb = sub?.Frame(cx);
            if (sb != sub)
                r += (Sub, sb);
            // don't worry about TableRow
            return cx.Add(r,true);
        }
    }
    internal class SqlStar : SqlValue
    {
        internal const long
            Prefix = -240; // Query
        internal static readonly SqlValue Default = 
            new SqlStar(Transaction.TransPos + 1, (Query)null);
        public Query prefix => (Query)mem[Prefix];
        public SqlStar(long dp, Query pre, BTree<long, object> m = null) 
            : base(dp, (m??BTree<long,object>.Empty)+(Prefix,pre)) { }
        protected SqlStar(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlStar operator+(SqlStar s,(long,object)x)
        {
            return (SqlStar)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlStar(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlStar(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = (SqlStar)base.Relocate(wr);
            var pr = prefix.Relocate(wr);
            if (pr != prefix)
                r += (Prefix, pr);
            return r;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            return qn;
        }
        public override string ToString()
        {
            return (prefix==null)?"*":("("+prefix.ToString()+").*");
        }
    }
    internal class SqlTable : SqlValue
    {
        public SqlTable(long dp, string nm, Query fm, BTree<long, object> m = null)
            : base(dp, nm, (m ?? BTree<long, object>.Empty) + (_From, fm.defpos)
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
        internal override DBObject TableRef(Context cx, From f)
        {
            if (domain == Domain.Content && name == f.name)
            {
                var r = this + (_From, f) + (_Domain, f.domain);
                cx.Replace(this, r);
                return r;
            }
            return this;
        }
        internal override TypedValue Eval(Context cx)
        {
            return cx.cursors[from];
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            return qn;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (from!=-1L) { sb.Append(" Table:"); sb.Append(Uid(from)); }
            return sb.ToString();
        }
    }
    internal class SqlOldTable : SqlTable
    {
        public SqlOldTable(long dp, string nm, FromOldTable fo, BTree<long, object> m = null)
            : base(dp, nm, fo, (m ?? BTree<long, object>.Empty)
                  + (FromOldTable.TRSPos,fo.trs))
        { }
        protected SqlOldTable(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlOldTable operator +(SqlOldTable t, (long, object) x)
        {
            return (SqlOldTable)t.New(t.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlOldTable(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlOldTable(dp, mem);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" For:"); sb.Append(Uid(from));
            return sb.ToString();
        }
    }
    internal class SqlCopy : SqlValue
    {
        internal const long
            CopyFrom = -284; // long
        public long copyFrom => (long)mem[CopyFrom];
        public SqlCopy(long dp, string nm, ObInfo sc, long fp, long cp,
            BTree<long, object> m = null)
            : base(dp, _Mem(fp,m) + (Name, nm)
         + (CopyFrom, cp) + (_Domain, sc.domain) + 
                  (Info,sc.Relocate(dp)))
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
        internal override bool IsFrom(Query q, bool ordered = false, Domain ut = null)
        {
            return q.rowType[defpos] != null;
        }
        internal override long Defpos()
        {
            return copyFrom;
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlCopy(dp, mem);
        }
        internal override SqlValue Reify(Context cx, Ident ic)
        {
            if (defpos >= Transaction.Compiling && ic.iix < Transaction.Compiling)
                return new SqlCopy(ic.iix, ic.ToString(), info, from, copyFrom);
            else
                return this;
        }
        /*      internal override TypedValue Eval(Context _cx)
              {
                  if (!_cx.from.Contains(defpos))
                      throw new PEException("PE189");
                  var p = _cx.from[defpos];
                  var cu = _cx.cursors[p];
                  var v = cu[defpos];
                  return v;
              } */
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            return (qn[copyFrom]==null)? qn+(qn.Length,this) : qn;
        }
        internal override bool sticky()
        {
            return true;
        }
        public override string ToString()
        {
            return base.ToString() + " copy from "+Uid(copyFrom);
        }
    }
    /// <summary>
    /// We take some trouble over evaluating SqlCols, to ensure that several queries
    /// can access the same table without interfering with each other.
    /// Perhaps this is overkill..
    /// </summary>
    internal class SqlStructCol : SqlValue
    {
        public SqlStructCol(long dp, string nm, long op, BTree<long, object> m = null)
            : base(dp, (m ?? BTree<long, object>.Empty) + (Name, nm) + (_From, op)) { }
        protected SqlStructCol(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlStructCol operator +(SqlStructCol s, (long, object) x)
        {
            return (SqlStructCol)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlStructCol(defpos, m);
        }
        internal override bool IsFrom(Query q, bool ordered = false, Domain ut = null)
        {
            return q.defpos!=-1L && q.defpos == from;
        }
        internal override TypedValue Eval(Context _cx)
        {
            var pa = _cx.obs[from] as SqlValue;
            var sv = pa.Eval(_cx);
            if (sv?.IsNull !=false)
                throw new DBException("22004",pa?.name??"??");
            return sv[defpos];
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            return qn;
        }
    }
    internal class SqlRowSetCol : SqlValue
    {
        public SqlRowSetCol(ObInfo oi, long rp)
            : base(oi.defpos, BTree<long, object>.Empty + (Name, oi.name) + (_From, rp)
                  + (_Domain, oi.domain) + (Info, oi))
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
        internal override Selection Needs(Selection qn)
        {
            return qn;
        }
    }
    internal class SqlTableCol : SqlValue
    {
        internal const long
            TableCol = -322; // TableColumn
        public TableColumn tableCol => (TableColumn)mem[TableCol];
        public SqlTableCol(long dp, string nm, long qp, TableColumn tc, BTree<long, object> m = null)
            : base(dp,(m ?? BTree<long, object>.Empty) + (TableCol, tc)
                  + (_Domain, tc.domain) + (Name, nm) + (_From, qp)
                  + (Info,new ObInfo(dp,nm,tc.domain)))
        { }
        protected SqlTableCol(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlTableCol operator +(SqlTableCol s, (long, object) x)
        {
            return (SqlTableCol)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlTableCol(defpos, m);
        }
        internal override bool IsFrom(Query q, bool ordered = false, Domain ut = null)
        {
            return q.defpos != -1L && q.defpos == from;
        }
        internal override SqlValue Reify(Context cx, Ident ic)
        {
            return new SqlCopy(ic.iix, ic.ToString(), info, from, tableCol.defpos);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlTableCol(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = (SqlTableCol)base.Relocate(wr);
            var tc = tableCol.Relocate(wr);
            if (tc != tableCol)
                r += (TableCol, tc);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SqlTableCol)base.Frame(cx);
            if (tableCol != null)
                r += (TableCol, tableCol.Frame(cx));
            return cx.Add(r,true);
        }
        internal override TypedValue Eval(Context _cx)
        {
            if (!_cx.from.Contains(defpos))
                throw new PEException("PE189");
            if (_cx.cursors[_cx.from[defpos]] is Cursor cu)
                return cu.values[defpos];
            return base.Eval(_cx);
        }
        internal override bool Uses(long t)
        {
            return tableCol?.tabledefpos == t;
        }
        internal override bool sticky()
        {
            return true;
        }
        internal override long Defpos()
        {
            return tableCol?.defpos ?? base.Defpos();
        }
        /// <summary>
        /// We are a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            return base.Needs(qn); // correct
        }
        internal override long target => tableCol.defpos;
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(TableCol))
            { sb.Append(" Col: "); sb.Append(Uid(tableCol.defpos)); }
            return sb.ToString();
        }
    }
    internal class SqlOldRowCol : SqlTableCol
    {
        internal const long
            TransitionRowSet = -318; // TransitionRowSet
        internal TransitionRowSet trs => (TransitionRowSet)mem[TransitionRowSet];
        internal long trsPos => (long)(mem[FromOldTable.TRSPos] ?? -1L);
        public SqlOldRowCol(long dp,string name, FromOldTable fo, TableColumn tc) 
            : this(dp, name, fo.defpos, fo.trs, tc) { }
        public SqlOldRowCol(long dp, string name, long fop, long trp, TableColumn tc)
            : base(dp, name, fop, tc, new BTree<long, object>(FromOldTable.TRSPos, trp)) { }
        protected SqlOldRowCol(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlOldRowCol operator+(SqlOldRowCol s,(long,object)x)
        {
            return (SqlOldRowCol)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlOldRowCol(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlOldRowCol(dp,mem);
        }
        internal override SqlValue Reify(Context cx, Ident ic)
        {
            return new SqlOldRowCol(ic.iix, name, from, trsPos, tableCol);
        }
        internal override TypedValue Eval(Context _cx)
        {
            return _cx.cursors[tableCol.tabledefpos]?[tableCol.defpos];
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(' ');sb.Append(Uid(defpos));
            sb.Append(" TRS:"); sb.Append(trs);
            return sb.ToString();
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
        /// <param name="cx">the context</param>
        internal SqlTypeExpr(long dp,Domain ty,Context cx)
            : base(dp,new BTree<long,object>(TreatType,ty))
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
        internal override Selection Needs(Selection qn)
        {
            return qn;
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (SqlTypeExpr)base.Relocate(wr);
            var t = (Domain)type.Relocate(wr);
            if (t != type)
                r += (TreatType, t);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SqlTypeExpr)base.Frame(cx);
            var t = (Domain)type.Frame(cx);
            if (t != type)
                r += (TreatType, t);
            return cx.Add(r,true);
        }
    }
    /// <summary>
    /// A Subtype value for use in TREAT
    /// </summary>
    internal class SqlTreatExpr : SqlValue
    {
        internal const long
            TreatExpr = -313; // SqlValue
        SqlValue val => (SqlValue)mem[TreatExpr];
        /// <summary>
        /// constructor: a new Treat expression
        /// </summary>
        /// <param name="ty">the type</param>
        /// <param name="cx">the context</param>
        internal SqlTreatExpr(long dp,SqlValue v,Domain ty, Context cx)
            : base(dp,BTree<long,object>.Empty+(_Domain,(ty.kind==Sqlx.ONLY && ty.iri!=null)?
                  new Domain(dp,v.domain.kind,v.domain.mem+(Domain.Iri,ty.iri)):ty)
                  +(TreatExpr,v))
        { }
        protected SqlTreatExpr(long dp, BTree<long, object> m) : base(dp, m) { }
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (SqlTreatExpr)base.Relocate(wr);
            var te = (SqlValue)val.Relocate(wr);
            if (te != val)
                r += (TreatExpr, te);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SqlTreatExpr)base.Frame(cx);
            var te = (SqlValue)val.Frame(cx);
            if (te != val)
                r += (TreatExpr, te);
            return cx.Add(r,true);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (SqlTreatExpr)base.AddFrom(cx, q);
            var a = val.AddFrom(cx, q);
            if (a != val)
                r += (TreatExpr, a);
            return (SqlValue)cx.Add(r);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlTreatExpr)base._Replace(cx,so,sv);
            var v = (SqlValue)r.val._Replace(cx,so,sv);
            if (v != r.val)
                r += (TreatExpr, v);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool Uses(long t)
        {
            return val.Uses(t);
        }
        /// <summary>
        /// The value had better fit the specified type
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (val.Eval(cx)?.NotNull() is TypedValue tv)
            {
                if (!domain.HasValue(tv))
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
        internal override Selection Needs(Selection qn)
        {
            return val.Needs(qn);
        }
        public override string ToString()
        {
            return "TREAT(..)";
        }
    }
    internal class SqlElement : SqlValue
    {
        internal SqlElement(long defpos,string nm,long op,Domain dt) 
            : base(defpos,nm,dt,BTree<long,object>.Empty+(_From,op))
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
            return left.Eval(cx)[defpos];
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            return qn;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(left.ToString());
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
        public override Sqlx kind => (Sqlx)mem[Kind];
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
        public SqlValueExpr(long dp, Sqlx op, SqlValue lf, SqlValue rg, 
            Sqlx m, BTree<long, object> mm = null)
            : base(dp, _Type(dp, op, m, lf, rg, mm)
                  + (Left, lf) + (Right, rg) + (Modifier, m) + (Kind, op)
                  +(Dependents,new BTree<long,bool>(lf?.defpos??-1L,true)+(rg?.defpos??-1L,true))
                  +(Depth,1+_Max((lf?.depth??0),(rg?.depth??0))))
        { }
        protected SqlValueExpr(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlValueExpr operator +(SqlValueExpr s, (long, object) x)
        {
            var (p, v) = x;
            s = new SqlValueExpr(s.defpos, s.mem + x);
            if (p == Left || p == Right)
                s += (_Domain, (Domain)_Type(s.defpos, s.kind, s.mod, s.left, s.right)[_Domain]);
            return s;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlValueExpr(defpos, m);
        }
        internal override SqlValue AddReg(Context cx, long rs, TRow key)
        {
            left?.AddReg(cx,rs,key);
            right?.AddReg(cx,rs,key);
            return this;
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlValueExpr(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = (SqlValueExpr)base.Relocate(wr);
            var lf = left?.Relocate(wr);
            if (lf != left)
                r += (Left, lf);
            var rg = right?.Relocate(wr);
            if (rg != right)
                r += (Right, rg);
            return r;
        }
        internal override (SqlValue,Query) Resolve(Context cx, Query fm)
        {
            SqlValue lf=null, rg=null, r = this;
            (lf,fm) = left?.Resolve(cx, fm)??(lf,fm);
            var rt = fm?.rowType;
            if (kind == Sqlx.DOT && lf is SqlTable st && st.from==fm?.defpos 
                && (rt?.map.Contains(right.name)==true))
            {
                var i = rt.map[right.name].Value;
                var sc = (SqlCopy)rt[i];
                var nn = (left.alias ?? left.name) + "." + (right.alias ?? right.name);
                if (alias != null)
                    nn = alias;
                var nc = sc + (_Alias,nn)+(Info,sc.info+(Name,nn));
                if (cx.obs[sc.from] is Query qs && nc.defpos!=sc.defpos)
                    cx.Replace(qs,qs.AddMatchedPair(nc, sc));
                fm += (Query.RowType,rt + (i, nc));
                return (nc,fm);
            }
            (rg,fm) = right?.Resolve(cx, fm)??(rg,fm);
            if (lf != left || rg != right)
                r = (SqlValue)cx.Replace(this,
                    new SqlValueExpr(defpos, kind, lf, rg, mod, mem));
            return (r,fm);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlValueExpr)base._Replace(cx,so,sv);
            var lf = r.left?._Replace(cx,so,sv);
            if (lf != r.left)
                r += (Left, lf);
            var rg = (SqlValue)r.right?._Replace(cx,so,sv);
            if (rg != r.right)
                r += (Right, rg);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SqlValueExpr)base.Frame(cx);
            var lf = r.left?.Frame(cx);
            if (lf != r.left)
                r += (Left, lf);
            var rg = (SqlValue)r.right?.Frame(cx);
            if (rg != r.right)
                r += (Right, rg);
            return cx.Add(r,true);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (SqlValueExpr)base.AddFrom(cx, q);
            var a = r.left.AddFrom(cx, q);
            if (a != r.left)
                r += (Left, a);
            a = r.right.AddFrom(cx, q);
            if (a != r.right)
                r += (Right, a);
            return (SqlValue)cx.Add(r);
        }
        internal override DBObject TableRef(Context cx, From f)
        {
            DBObject r = this;
            if (kind==Sqlx.DOT)
            {
                var lf = (SqlValue)left?.TableRef(cx, f);
                var rg = (SqlValue)right?.TableRef(cx, f);
                if (lf != left || rg != right)
                {
                    r = new SqlValueExpr(defpos, kind, lf, rg, mod, mem);
                    cx.done += (defpos, r);
                }
            }
            return r;
        }
        internal override bool Uses(long t)
        {
            return (left?.Uses(t)==true) || (right?.Uses(t)==true);
        }
        internal override BTree<long, SqlValue> Disjoin()
        { // parsing guarantees right associativity
            return (kind == Sqlx.AND)? right.Disjoin()+(left.defpos, left):base.Disjoin();
        }
         internal override SqlValue Import(Query q)
        {
            if (left.Import(q) is SqlValue a)
            {
                var b = right?.Import(q);
                if (left == a && right == b)
                    return this;
                if (right == null || b != null)
                    return new SqlValueExpr(defpos, kind, a, b, mod) + (_Alias, alias);
            }
            return null;
        }
        internal override SqlValue Reify(Context cx,ObInfo oi)
        {
            return new SqlValueExpr(defpos, kind, left.Reify(cx,oi),right.Reify(cx,oi), mod);
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
            Domain tr = (right == null) ? dt : right.FindType(dt);
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
                    return tl.elType.domain;
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
        internal override void _AddReqs(Transaction tr, Query gf, Domain ut, 
            ref BTree<SqlValue, int> gfreqs, int i)
        {
            left.AddReqs(tr, gf, ut, ref gfreqs, i);
            right?.AddReqs(tr, gf, ut, ref gfreqs, i);

        }
        const int ea = 1, eg = 2, la = 4, lr = 8, lg = 16, ra = 32, rr = 64, rg = 128;
        internal override SqlValue _ColsForRestView(long dp,Context cx,
            From gf, GroupSpecification gs, ref BTree<SqlValue, string> gfc, 
            ref BTree<SqlValue, string> rem, ref BTree<string, bool?> reg, 
            ref BTree<long, SqlValue> map)
        {
            var rgl = BTree<string, bool?>.Empty;
            var gfl = BTree<SqlValue, string>.Empty;
            var rel = BTree<SqlValue, string>.Empty;
            var rgr = BTree<string, bool?>.Empty;
            var gfr = BTree<SqlValue, string>.Empty;
            var rer = BTree<SqlValue, string>.Empty;
            // we distinguish many cases here using the above constants: exp/left/right:agg/grouped/remote
            int cse = 0, csa = 0;
            SqlValue el = left, er = right;
            if (((Query)cx.obs[gf.QuerySpec(cx)]).aggregates())
                cse += ea;
            if (gs?.Has(this) == true)
                cse += eg;
            if (left.aggregates())
                cse += la;
            if (right?.aggregates() == true)
                cse += ra;
            var cs = gf.source as CursorSpecification;
            if (left.IsFrom(gf) && (!left.isConstant))
            {
                cse += lr;
                el = left._ColsForRestView(dp, cx, gf, gs, ref gfl, ref rel, ref rgl, ref map);
            }
            if (right?.IsFrom(gf) == true && (!right.isConstant))
            {
                cse += rr;
                er = right._ColsForRestView(dp, cx, gf, gs, ref gfr, ref rer, ref rgr, ref map);
            }
            if (left.Grouped(gs))
                cse += lg;
            if (right?.Grouped(gs) == true)
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
                       /* for (var b = needed.First(); b != null; b = b.Next())
                        {
                            var sv = b.key();
                            var id = sv.alias ?? cx.idents[sv.defpos].ident ?? ("C_" + sv.defpos);
                            gfc +=(sv, id);
                            rem +=(sv, id);
                            if (aggregates())
                                reg+=(id, true);
                        } */
                        return base._ColsForRestView(dp, cx, gf, gs, ref gfc, ref rem, ref reg, ref map);
                    }
            }
            gfc = BTree<SqlValue, string>.Empty;
            rem = BTree<SqlValue, string>.Empty;
            reg = BTree<string, bool?>.Empty;
            SqlValueExpr se = this;
            SqlValue st = null;
            var nn = alias;
            var nl = el?.alias;
            var nr = er?.alias;
            switch (csa)
            {
                case 1: // lr rr : QS->Cexp as exp ; CS->Left’ op right’ as Cexp
                    // rel and rer will have just one entry each
                    st = new SqlValue(dp,nn);
                    rem += (new SqlValueExpr(defpos, kind, rel.First().key(), rer.First().key(), mod,
                        new BTree<long, object>(_Alias, nn)), nn);
                    gfc += (st, nn);
                    map += (defpos, st);
                    return st;
                case 2: // lr: QS->Cleft op right as exp; CS->Left’ as Cleft 
                    // rel will have just one entry, rer will have 0 entries
                    se = new SqlValueExpr(defpos, kind,
                        new SqlValue(dp,nl), right, mod,
                        new BTree<long, object>(_Alias, alias));
                    rem += (rel.First().key(), nl);
                    gfc += (gfl.First().key(), nl);
                    map += (defpos, se);
                    return se;
                case 3:// rr: QS->Left op Cright as exp; CS->Right’ as CRight
                    // rer will have just one entry, rel will have 0 entries
                    se = new SqlValueExpr(defpos, kind, left,
                        new SqlValue(dp,er.alias?? er.name),  
                        mod, new BTree<long, object>(_Alias, alias));
                    rem += (rer.First().key(), nr);
                    gfc += (gfr.First().key(), nr);
                    map += (defpos, se);
                    return se;
                case 4: // ea lr rr: QS->SCleft op SCright; CS->Left’ as Cleft,right’ as Cright
                    // gfl, gfr, rgl and rgr may have sevral entries: we need all of them
                    se = new SqlValueExpr(defpos, kind, el, er, mod, new BTree<long, object>(_Alias, nn));
                    gfc += gfl; gfc += gfr; rem += rel; rem += rer;
                    map += (defpos, se);
                    return se;
                case 5: // ea eg lr rr: QS->Cexp as exp  group by exp; CS->Left’ op right’ as Cexp group by Cexp
                    // rel and rer will have just one entry each
                    reg += (nn, true);
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
                    se = new SqlValueExpr(defpos, kind, el, er, mod, new BTree<long, object>(_Alias, alias));
                    gfc += gfl; rem += rel;
                    map += (defpos, se);
                    return se;
                case 10: // ea lr lg rg: QS->Left op SCright as exp group by left; CS->Left’ as Cleft,right’ as Cright group by Cleft
                    se = new SqlValueExpr(defpos, kind, el, er, mod, new BTree<long, object>(_Alias, alias));
                    gfc += gfr; rem += rer;
                    map += (defpos, se);
                    return se;
                case 11: // ea la lr rg: QS->SCleft op right as exp group by right; CS->Left’ as Cleft
                    se = new SqlValueExpr(defpos, kind, el, right, mod, new BTree<long, object>(_Alias, alias));
                    gfc += gfl; rem += rel;
                    map += (defpos, se);
                    break;
                case 12: // ea lg ra: QS->Left op SCright as exp group by left; CS->Right’ as Cright
                    se = new SqlValueExpr(defpos, kind, left, er, mod, new BTree<long, object>(_Alias, alias));
                    gfc += gfr; rem += rer;
                    break;
            }
            se = new SqlValueExpr(defpos, kind, el, er, mod, new BTree<long, object>(_Alias, nn));
            if (gs.Has(this))// what we want if grouped
                st = new SqlValue(dp,nn);
            if (gs.Has(this))
            {
                rem += (se, se.alias);
                gfc += (se, alias);
            }
            else
            {
                if (!el.isConstant)
                    gfc += (el, nl);
                if (!er.isConstant)
                    gfc += (er, nr);
            }
            map += (defpos, se);
            return se;
        }
        void CopyFrom(ref BTree<string, bool?> dst, BTree<SqlValue, string> sce)
        {
            for (var b = sce.First(); b != null; b = b.Next())
            {
                var sv = b.key();
                dst +=(sv.alias ?? sv.name, true);
            }
        }
        void GroupOperands(ref BTree<string, bool?> dst, BTree<SqlValue, string> sce)
        {
            for (var b = sce.First(); b != null; b = b.Next())
                if (b.key().Operand() is SqlValue sv)
                    dst +=(sv.alias ?? sv.name, true);
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            left?.Build(_cx,rs);
            right?.Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx,RowSet rs)
        {
            left?.StartCounter(_cx,rs);
            right?.StartCounter(_cx,rs);
        }
        internal override void _AddIn(Context _cx,Cursor rb, TRow key,
            ref BTree<long, bool?> aggsDone)
        {
            left?._AddIn(_cx,rb, key, ref aggsDone);
            right?._AddIn(_cx,rb, key, ref aggsDone);
        }
        internal override void OnRow(Cursor bmk)
        {
            left?.OnRow(bmk);
            right?.OnRow(bmk);
        }
        /// <summary>
        /// Analysis stage Conditions: set up join conditions.
        /// Code for altering ordSpec has been moved to Analysis stage Orders.
        /// </summary>
        /// <param name="j">a join part</param>
        /// <returns></returns>
        internal override Query JoinCondition(Context cx, JoinPart j,
            ref BTree<long, SqlValue> joinCond, ref BTree<long, SqlValue> where)
        // update j.joinCondition, j.thetaCondition and j.ordSpec
        {
            switch (kind)
            {
                case Sqlx.AND:
                    j = (JoinPart)left.JoinCondition(cx, j, ref joinCond, ref where);
                    j = (JoinPart)right.JoinCondition(cx, j, ref joinCond, ref where);
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
                            if (right.IsFrom(j.left, false) && right.defpos > 0)
                            {
                                j.left.AddMatch(cx, right, left.Eval(cx));
                                return j;
                            }
                            else if (right.IsFrom(j.right, false) && right.defpos > 0)
                            {
                                j.right.AddMatch(cx, right, left.Eval(cx));
                                return j;
                            }
                            break;
                        }
                        if (right.isConstant)
                        {
                            if (kind != Sqlx.EQL)
                                break;
                            if (left.IsFrom(j.left, false) && left.defpos > 0)
                            {
                                j.left.AddMatch(cx, left, right.Eval(cx));
                                return j;
                            }
                            else if (left.IsFrom(j.right, false) && left.defpos > 0)
                            {
                                j.right.AddMatch(cx, left, right.Eval(cx));
                                return j;
                            }
                            break;
                        }
                        var ll = left.IsFrom(j.left, true);
                        var rr = right.IsFrom(j.right, true);
                        if (ll && rr)
                            return j += (JoinPart.JoinCond, joinCond += (defpos, this));
                        var rl = right.IsFrom(j.left, true);
                        var lr = left.IsFrom(j.right, true);
                        if (rl && lr)
                        {
                            var nv = new SqlValueExpr(defpos, Sqlx.EQL, right, left, mod);
                            return j += (JoinPart.JoinCond, joinCond + (nv.defpos, nv));
                        }
                        break;
                    }
            }
            return base.JoinCondition(cx, j, ref joinCond, ref where);
        }
        /// <summary>
        /// Analysis stage Conditions: distribute conditions to joins and froms.
        /// OR expressions cannot be distributed
        /// </summary>
        /// <param name="q">the query</param>
        /// <param name="repl">updated list of potential replacements (because of equality)</param>
        internal override Query DistributeConditions(Context cx, Query q, BTree<SqlValue, SqlValue> repl)
        {
            q = base.DistributeConditions(cx, q, repl);
            switch (kind)
            {
                case Sqlx.NO: return left.DistributeConditions(cx, q, repl);
                case Sqlx.AND:
                    q = left.DistributeConditions(cx, q, repl);
                    return right.DistributeConditions(cx, q, repl);
                case Sqlx.EQL:
                    if (IsFrom(q, false))
                        return q.AddCondition(cx,new BTree<long, SqlValue>(defpos, this), null, null);
                    else if (repl[left] is SqlValue lr && lr.IsFrom(q, false))
                    {
                        var ns = new SqlValueExpr(defpos, kind, repl[left], right, Sqlx.NO);
                        return q.AddCondition(cx, new BTree<long, SqlValue>(ns.defpos, ns), null,null);
                    }
                    else if (repl[right] is SqlValue rl && rl.IsFrom(q, false))
                    {
                        var ns = new SqlValueExpr(defpos, kind, repl[right], left, Sqlx.NO);
                        return q.AddCondition(cx, new BTree<long, SqlValue>(ns.defpos, ns), null,null);
                    }
                    return q;
                case Sqlx.GTR: goto case Sqlx.EQL;
                case Sqlx.LSS: goto case Sqlx.EQL;
                case Sqlx.NEQ: goto case Sqlx.EQL;
                case Sqlx.LEQ: goto case Sqlx.EQL;
                case Sqlx.GEQ: goto case Sqlx.EQL;
            }
            return q;
        }
        internal override SqlValue PartsIn(Selection dt)
        {
            var lf = left.PartsIn(dt);
            var rg = right.PartsIn(dt);
            if (lf == null)
                return (kind == Sqlx.AND) ? rg : null;
            if (rg == null)
                return (kind == Sqlx.AND) ? lf : null;
            return new SqlValueExpr(defpos, kind, lf, rg, mod);
        }
        internal override void BuildRepl(Query lf, ref BTree<SqlValue, SqlValue> lr, ref BTree<SqlValue, SqlValue> rr)
        {
            switch (kind)
            {
                case Sqlx.NO: left.BuildRepl(lf, ref lr, ref rr); return;
                case Sqlx.AND:
                    left.BuildRepl(lf, ref lr, ref rr);
                    right.BuildRepl(lf, ref lr, ref rr); return;
                case Sqlx.EQL:
                    if (left.IsFrom(lf, false))
                    {
                        rr += (left, right);
                        lr += (right, left);
                    }
                    else
                    {
                        lr += (left, right);
                        rr += (right, left);
                    }
                    return;
            }
        }
        /// <summary>
        /// Evaluate the expression (binary operators).
        /// May return null if operands not yet ready
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            TypedValue v = null;
            try
            {
                switch (kind)
                {
                    case Sqlx.AND:
                        {
                            var a = left.Eval(cx)?.NotNull();
                            var b = right.Eval(cx)?.NotNull();
                            if (a == null || b == null)
                                return null;
                            if (mod == Sqlx.BINARY) // JavaScript
                                v = new TInt(a.ToLong() & b.ToLong());
                            else
                                v = (a.IsNull || b.IsNull) ?
                                    domain.defaultValue :
                                    TBool.For(((TBool)a).value.Value && ((TBool)b).value.Value);
                            return v;
                        }
                    case Sqlx.ASC: // JavaScript ++
                        {
                            v = left.Eval(cx)?.NotNull();
                            if (v == null)
                                return null;
                            if (v.IsNull)
                                return domain.defaultValue;
                            var w = v.dataType.Eval(defpos,v, Sqlx.ADD, new TInt(1L));
                            return (mod == Sqlx.BEFORE) ? w : v;
                        }
                    case Sqlx.ASSIGNMENT:
                        {
                            var a = left;
                            var b = right.Eval(cx)?.NotNull();
                            if (b == null)
                                return null;
                            if (a == null)
                                return b;
                            return v;
                        }
                    case Sqlx.COLLATE:
                        {
                            var a = left.Eval(cx)?.NotNull();
                            object o = a?.Val();
                            if (o == null)
                                return null;
                            Domain ct = left.domain;
                            if (ct.kind == Sqlx.CHAR)
                            {
                                var b = right.Eval(cx)?.NotNull();
                                if (b == null)
                                    return null;
                                string cname = b?.ToString();
                                if (ct.culture.Name == cname)
                                    return left.Eval(cx)?.NotNull();
                                Domain nt = new Domain(defpos,ct.kind, BTree<long, object>.Empty
                                    + (Domain.Precision, ct.prec) + (Domain.Charset, ct.charSet)
                                    + (Domain.Culture, new CultureInfo(cname)));
                                return new TChar(nt, (string)o);
                            }
                            throw new DBException("2H000", "Collate on non-string?").ISO();
                        }
                    case Sqlx.COMMA: // JavaScript
                        {
                            if (left.Eval(cx)?.NotNull() == null)// for side effects
                                return null;
                            return right.Eval(cx);
                        }
                    case Sqlx.CONCATENATE:
                        {
                            if (left.domain.kind == Sqlx.ARRAY
                                && right.domain.kind == Sqlx.ARRAY)
                                return left.domain.Concatenate((TArray)left.Eval(cx),
                                    (TArray)right.Eval(cx));
                            var lf = left.Eval(cx)?.NotNull();
                            var or = right.Eval(cx)?.NotNull();
                            if (lf == null || or == null)
                                return null;
                            var stl = lf.ToString();
                            var str = or.ToString();
                            return new TChar(or.dataType, (lf.IsNull && or.IsNull) ? null : stl + str);
                        }
                    case Sqlx.CONTAINS:
                        {
                            var ta = left.Eval(cx)?.NotNull();
                            if (ta == null)
                                return null;
                            var a = ta.Val() as Period;
                            if (a == null)
                                return domain.defaultValue;
                            if (right.domain.kind == Sqlx.PERIOD)
                            {
                                var tb = right.Eval(cx)?.NotNull();
                                if (tb == null)
                                    return null;
                                var b = tb.Val() as Period;
                                if (b == null)
                                    return TBool.Null;
                                return TBool.For(a.start.CompareTo(b.start) <= 0
                                    && a.end.CompareTo(b.end) >= 0);
                            }
                            var c = right.Eval(cx)?.NotNull();
                            if (c == null)
                                return null;
                            if (c == TNull.Value)
                                return TBool.Null;
                            return TBool.For(a.start.CompareTo(c) <= 0 && a.end.CompareTo(c) >= 0);
                        }
                    case Sqlx.DESC: // JavaScript --
                        {
                            v = left.Eval(cx)?.NotNull();
                            if (v == null)
                                return null;
                            if (v.IsNull)
                                return domain.defaultValue;
                            var w = v.dataType.Eval(defpos,v, Sqlx.MINUS, new TInt(1L));
                            return (mod == Sqlx.BEFORE) ? w : v;
                        }
                    case Sqlx.DIVIDE:
                        v = domain.Eval(defpos,left.Eval(cx)?.NotNull(), kind,
                            right.Eval(cx)?.NotNull());
                        return v;
                    case Sqlx.DOT:
                        v = left.Eval(cx);
                        if (v != null)
                            v = v[right.defpos];
                        return v;
                    case Sqlx.EQL:
                        {
                            var rv = right.Eval(cx)?.NotNull();
                            if (rv == null)
                                return null;
                            return TBool.For(rv != null
                                && rv.CompareTo(left.Eval(cx)?.NotNull()) == 0);
                        }
                    case Sqlx.EQUALS:
                        {
                            var ta = left.Eval(cx)?.NotNull();
                            var tb = right.Eval(cx)?.NotNull();
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
                            var ta = left.Eval(cx) as TMultiset;
                            var tb = right.Eval(cx) as TMultiset;
                            if (ta == null || tb == null)
                                return null;
                            return left.domain.Coerce(TMultiset.Except(ta, tb, mod == Sqlx.ALL));
                        }
                    case Sqlx.GEQ:
                        {
                            var ta = left.Eval(cx)?.NotNull();
                            var tb = right.Eval(cx)?.NotNull();
                            if (ta == null || tb == null || ta.IsNull || tb.IsNull)
                                return null;
                            return TBool.For(ta.CompareTo(tb) >= 0);
                        }
                    case Sqlx.GTR:
                        {
                            var ta = left.Eval(cx)?.NotNull();
                            var tb = right.Eval(cx)?.NotNull();
                            if (ta == null || tb == null || ta.IsNull || tb.IsNull)
                                return null;
                            return TBool.For(ta.CompareTo(tb) > 0);
                        }
                    case Sqlx.INTERSECT:
                        {
                            var ta = left.Eval(cx) as TMultiset;
                            var tb = right.Eval(cx) as TMultiset;
                            if (ta == null || tb == null)
                                return null;
                            return left.domain.Coerce(TMultiset.Intersect(ta, tb, mod == Sqlx.ALL));
                        }
                    case Sqlx.LBRACK:
                        {
                            var al = left.Eval(cx)?.NotNull();
                            var ar = right.Eval(cx)?.NotNull();
                            if (al == null || ar == null)
                                return null;
                            var sr = ar.ToInt();
                            if (al.IsNull || !sr.HasValue)
                                return domain.defaultValue;
                            return ((TArray)al)[sr.Value];
                        }
                    case Sqlx.LEQ:
                        {
                            var ta = left.Eval(cx)?.NotNull();
                            var tb = right.Eval(cx)?.NotNull();
                            if (ta == null || tb == null || ta.IsNull || tb.IsNull)
                                return null;
                            return TBool.For(ta.CompareTo(tb) <= 0);
                        }
                    case Sqlx.LOWER: // JavScript >> and >>>
                        {
                            long a;
                            var ol = left.Eval(cx)?.NotNull();
                            var or = right.Eval(cx)?.NotNull();
                            if (ol == null || or == null)
                                return null;
                            if (or.IsNull)
                                return domain.defaultValue;
                            var s = (byte)(or.ToLong().Value & 0x1f);
                            if (mod == Sqlx.GTR)
                                unchecked
                                {
                                    a = (long)(((ulong)ol.Val()) >> s);
                                }
                            else
                            {
                                if (ol.IsNull)
                                    return domain.defaultValue;
                                a = ol.ToLong().Value >> s;
                            }
                            v = new TInt(a);
                            return v;
                        }
                    case Sqlx.LSS:
                        {
                            var ta = left.Eval(cx)?.NotNull();
                            var tb = right.Eval(cx)?.NotNull();
                            if (ta == null || tb == null || ta.IsNull || tb.IsNull)
                                return null;
                            return TBool.For(ta.CompareTo(tb) < 0);
                        }
                    case Sqlx.MINUS:
                        {
                            var tb = right.Eval(cx)?.NotNull();
                            if (tb == null)
                                return null;
                            if (left == null)
                            {
                                v = right.domain.Eval(defpos,new TInt(0), Sqlx.MINUS, tb);
                                return v;
                            }
                            var ta = left.Eval(cx)?.NotNull();
                            if (ta == null)
                                return null;
                            v = left.domain.Eval(defpos,ta, kind, tb);
                            return v;
                        }
                    case Sqlx.NEQ:
                        {
                            var rv = right.Eval(cx)?.NotNull();
                            return TBool.For(left.Eval(cx)?.NotNull().CompareTo(rv) != 0);
                        }
                    case Sqlx.NO: return left.Eval(cx);
                    case Sqlx.NOT:
                        {
                            var ta = left.Eval(cx)?.NotNull();
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
                            var ta = left.Eval(cx)?.NotNull();
                            var tb = right.Eval(cx)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            switch (mod)
                            {
                                case Sqlx.BINARY: v = new TInt(ta.ToLong() | tb.ToLong()); break;
                                case Sqlx.EXCEPT: v = new TInt(ta.ToLong() ^ tb.ToLong()); break;
                                default:
                                    {
                                        if (ta.IsNull || tb.IsNull)
                                            return domain.defaultValue;
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
                            var ta = left.Eval(cx)?.NotNull();
                            var tb = right.Eval(cx)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            var a = ta.Val() as Period;
                            var b = tb.Val() as Period;
                            if (a == null || b == null)
                                return domain.defaultValue;
                            return TBool.For(a.end.CompareTo(b.start) >= 0
                                && b.end.CompareTo(a.start) >= 0);
                        }
                    case Sqlx.PERIOD:
                        {
                            var ta = left.Eval(cx)?.NotNull();
                            var tb = right.Eval(cx)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return new TPeriod(Domain.Period, new Period(ta, tb));
                        }
                    case Sqlx.PLUS:
                        {
                            var ta = left.Eval(cx)?.NotNull();
                            var tb = right.Eval(cx)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return left.domain.Eval(defpos,ta, kind, tb);
                        }
                    case Sqlx.PRECEDES:
                        {
                            var ta = left.Eval(cx)?.NotNull();
                            var tb = right.Eval(cx)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            var a = ta.Val() as Period;
                            var b = tb.Val() as Period;
                            if (a == null || b == null)
                                return domain.defaultValue;
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
                            var a = left.Eval(cx)?.NotNull();
                            var b = right.Eval(cx)?.NotNull();
                            if (a == null || b == null)
                                return null;
                            if (a.IsNull || b.IsNull)
                                return domain.defaultValue;
                            return ((TArray)a)[b.ToInt().Value];
                        }
                    case Sqlx.SUCCEEDS:
                        {
                            var ta = left.Eval(cx)?.NotNull();
                            var tb = right.Eval(cx)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            var a = ta.Val() as Period;
                            var b = tb.Val() as Period;
                            if (a == null || b == null)
                                return domain.defaultValue;
                            if (mod == Sqlx.IMMEDIATELY)
                                return TBool.For(a.start.CompareTo(b.end) == 0);
                            return TBool.For(a.start.CompareTo(b.end) >= 0);
                        }
                    case Sqlx.TIMES:
                        {
                            var ta = left.Eval(cx)?.NotNull();
                            var tb = right.Eval(cx)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            v = domain.Eval(defpos,ta, kind, tb);
                            return v;
                        }
                    case Sqlx.UNION:
                        {
                            var ta = left.Eval(cx)?.NotNull();
                            var tb = right.Eval(cx)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return left.domain.Coerce(
                                TMultiset.Union((TMultiset)ta, (TMultiset)tb, mod == Sqlx.ALL));
                        }
                    case Sqlx.UPPER: // JavaScript <<
                        {
                            var lf = left.Eval(cx)?.NotNull();
                            var or = right.Eval(cx)?.NotNull();
                            if (lf == null || or == null)
                                return null;
                            long a;
                            if (or.IsNull)
                                return domain.defaultValue;
                            var s = (byte)(or.ToLong().Value & 0x1f);
                            if (lf.IsNull)
                                return domain.defaultValue;
                            a = lf.ToLong().Value >> s;
                            v = new TInt(a);
                            return v;
                        }
                    //       case Sqlx.XMLATTRIBUTES:
                    //         return new TypedValue(left.domain, BuildXml(left) + " " + BuildXml(right));
                    case Sqlx.XMLCONCAT:
                        {
                            var ta = left.Eval(cx)?.NotNull();
                            var tb = right.Eval(cx)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return new TChar(left.domain, ta.ToString() + " " + tb.ToString());
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
        static BTree<long,object> _Type(long dp, Sqlx kind, Sqlx mod, SqlValue left, SqlValue right,
            BTree<long,object>mm = null)
        {
            var dm = Domain.Content;
            var nm = (string)mm?[Name]??""; 
            switch (kind)
            {
                case Sqlx.AND:
                    if (mod == Sqlx.BINARY) break; //JavaScript
                    dm = Domain.Bool; break;
                case Sqlx.ASC: goto case Sqlx.PLUS; // JavaScript
                case Sqlx.ASSIGNMENT: dm = right.domain; nm = left.name; break;
                case Sqlx.COLLATE: dm = Domain.Char; break;
                case Sqlx.COLON: dm = left.domain; nm = left.name; break;// JavaScript
                case Sqlx.CONCATENATE: dm = Domain.Char; break;
                case Sqlx.DESC: goto case Sqlx.PLUS; // JavaScript
                case Sqlx.DIVIDE:
                    {
                        var dl = left.domain.kind;
                        var dr = right.domain.kind;
                        if (dl == Sqlx.INTERVAL && (dr == Sqlx.INTEGER || dr == Sqlx.NUMERIC))
                        { dm = left.domain; break; }
                        dm = left.FindType(Domain.UnionNumeric); break;
                    }
                case Sqlx.DOT: dm = right.domain; nm = left.name + "." + right.name; break;
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
                            dm = left.FindType(Domain.UnionDateNumeric);
                        break;
                    }
                    dm = right.FindType(Domain.UnionDateNumeric); break;
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
                            dm = left.FindType(Domain.UnionDateNumeric);
                        break;
                    }
                case Sqlx.QMARK:
                        dm = Domain.Content; break;
                case Sqlx.RBRACK: dm = new Domain(dp,Sqlx.ARRAY, left.domain); break;
                case Sqlx.SET: dm = left.domain; nm = left.name; break; // JavaScript
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
                            dm = left.FindType(Domain.UnionNumeric);
                        break;
                    }
                case Sqlx.UNION: dm = left.domain; nm = left.name; break;
                case Sqlx.UPPER: dm = Domain.Int; break; // JavaScript <<
                case Sqlx.XMLATTRIBUTES: dm = Domain.Char; break;
                case Sqlx.XMLCONCAT: dm = Domain.Char; break;
            }
            return (mm??BTree<long, object>.Empty) + (_Domain, dm) + (Name, nm);
        }
        internal override SqlValue Invert()
        {
            switch (kind)
            {
                case Sqlx.AND:
                    return new SqlValueExpr(defpos, Sqlx.OR, left.Invert(),
         right.Invert(), Sqlx.NULL);
                case Sqlx.OR:
                    return new SqlValueExpr(defpos, Sqlx.AND, left.Invert(),
          right.Invert(), Sqlx.NULL);
                case Sqlx.NOT: return left;
                case Sqlx.EQL: return new SqlValueExpr(defpos, Sqlx.NEQ, left, right, Sqlx.NULL);
                case Sqlx.GTR: return new SqlValueExpr(defpos, Sqlx.LEQ, left, right, Sqlx.NULL);
                case Sqlx.LSS: return new SqlValueExpr(defpos, Sqlx.GEQ, left, right, Sqlx.NULL);
                case Sqlx.NEQ: return new SqlValueExpr(defpos, Sqlx.EQL, left, right, Sqlx.NULL);
                case Sqlx.GEQ: return new SqlValueExpr(defpos, Sqlx.LSS, left, right, Sqlx.NULL);
                case Sqlx.LEQ: return new SqlValueExpr(defpos, Sqlx.GTR, left, right, Sqlx.NULL);
            }
            return base.Invert();
        }
        /// <summary>
        /// Look to see if the given value expression is structurally equal to this one
        /// </summary>
        /// <param name="v">the SqlValue to test</param>
        /// <returns>whether they match</returns>
        internal override bool _MatchExpr(Query q,SqlValue v)
        {
            var e = v as SqlValueExpr;
            if (e == null || (domain != null && domain != v.domain))
                return false;
            if (left != null)
            {
                if (!left._MatchExpr(q,e.left))
                    return false;
            }
            else
                if (e.left != null)
                return false;
            if (right != null)
            {
                if (!right._MatchExpr(q,e.right))
                    return false;
            }
            else
                if (e.right != null)
                return false;
            return true;
        }
        internal override Query AddMatches(Query f)
        {
            if (kind == Sqlx.EQL)
                f = f.AddMatchedPair(left, right);
            return f;
        }
        /// <summary>
        /// analysis stage Conditions()
        /// </summary>
        internal override Query Conditions(Context cx, Query q, bool disj, out bool move)
        {
      //      var needed = BTree<SqlValue, int>.Empty;
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
                        q = left.Conditions(cx, q, false, out _);
                        q = right.Conditions(cx, q, false, out _);
                        break;
                    }
                case Sqlx.EQL:
                    {
                        if (!disj)
                            goto case Sqlx.OR;
                        if (right.isConstant && left.IsFrom(q, false) && left.defpos > 0)
                        {
                            q.AddMatch(cx, left, right.Eval(cx));
                            move = true;
                            return q;
                        }
                        else if (left.isConstant && right.IsFrom(q, false) && right.defpos > 0)
                        {
                            q.AddMatch(cx, right, left.Eval(cx));
                            move = true;
                            return q;
                        }
                        goto case Sqlx.AND;
                    }
                case Sqlx.NO:
                case Sqlx.NOT:
                    {
                        q = left.Conditions(cx, q, false, out _);
                        break;
                    }
            }
            if (q != null && domain == Domain.Bool)
                DistributeConditions(cx, q, BTree<SqlValue, SqlValue>.Empty);
            move = false;
            return q;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            var r = qn;
            if (kind!=Sqlx.DOT)
                r = left?.Needs(r) ?? r;
            r = right?.Needs(r) ?? r;
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(Flag()+Uid(defpos)+"(");
            if (left!=null)
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
            : base(-1,new BTree<long,object>(_Domain,Domain.Null))
        { }
        /// <summary>
        /// the value of null
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            return TNull.Value;
        }
        internal override bool _MatchExpr(Query q,SqlValue v)
        {
            return v is SqlNull;
        }
        internal override Query Conditions(Context cx, Query q, bool disj,out bool move)
        {
            move = true;
            return q;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            return qn;
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
        internal const long
            _Val = -317;// TypedValue
        internal TypedValue val=>(TypedValue)mem[_Val];
        internal readonly static SqlLiteral Null = new SqlLiteral(-1,TNull.Value);
        internal override long target => -1;
        /// <summary>
        /// Constructor: a Literal
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="ty">the kind of literal</param>
        /// <param name="v">the value of the literal</param>
        public SqlLiteral(long dp, TypedValue v, Domain td=null, ObInfo ts=null) 
            : base(_Pos(dp,ts),BTree<long, object>.Empty + (_Domain, td??v.dataType) 
                  + (_Val, v) +(Info,ts))
        { }
        public SqlLiteral(long dp, BTree<long, object> m) : base(dp, m) { }
        static long _Pos(long dp,ObInfo ts)
        {
            return (ts == null || ts.defpos < Transaction.TransPos) ? dp : ts.defpos;
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
        internal override bool _MatchExpr(Query q,SqlValue v)
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
            return val ?? domain.defaultValue;
        }
        public override int CompareTo(object obj)
        {
            var that = obj as SqlLiteral;
            if (that == null)
                return 1;
            return val?.CompareTo(that.val) ?? throw new PEException("PE000");
        }
        /// <summary>
        /// A literal is supplied by any query
        /// </summary>
        /// <param name="q">the query</param>
        /// <returns>true</returns>
        internal override bool IsFrom(Query q,bool ordered,Domain ut=null)
        {
            return true;
        }
        internal override SqlValue PartsIn(Selection dt)
        {
            return this;
        }
        internal override bool isConstant => true;
        internal override Domain FindType(Domain dt)
        {
            var vt = val.dataType;
            if (!dt.CanTakeValueOf(vt))
                throw new DBException("22005Y", dt.kind, vt.kind).ISO();
            if (vt.kind==Sqlx.INTEGER)
                return dt; // keep union options open
            return vt;
        }
        internal override void _AddReqs(Transaction tr, Query gf, Domain ut, ref BTree<SqlValue, int> gfreqs, int i)
        {
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            return qn;
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
        public SqlDateTimeLiteral(long dp, Domain op, string n)
            : base(dp, op.Parse(dp,n))
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
    // for row pseudofunctions
    internal class SqlTableRow : SqlValue
    {
        internal const long
            Property = -314; // string
        internal string property => (string)mem[Property];
        public SqlTableRow(long dp, string p) : base(dp,BTree<long,object>.Empty
            +(_Domain,_Dom(p))+(Property, p))
        { }
        protected SqlTableRow(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlTableRow operator+(SqlTableRow s,(long,object)x)
        {
            return new SqlTableRow(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlTableRow(defpos, m);
        }
        internal override bool sticky()
        {
            return true;
        }
        static Domain _Dom(string p)
        {
            switch(p)
            {
                case "SECURITY": return Domain._Level;
                case "VERSION": return Domain.Int;
                case "START": return Domain.Timestamp;
            }
            return Domain.Content;
        }
        internal override TypedValue Eval(Context cx)
        {
            return TNull.Value;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            return qn;
        }
    }
    /// <summary>
    /// A Row value
    /// </summary>
    internal class SqlRow : SqlValue
    {
        const long
            _Columns = 299; // BList<SqlValue>
        internal BList<SqlValue> columns =>
            (BList<SqlValue>) mem[_Columns] ?? BList<SqlValue>.Empty;
        public SqlRow(long dp, BTree<long, object> m) : base(dp, m) { }
        /// <summary>
        /// A row from the parser
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="r">the row</param>
        public SqlRow(long dp, BList<SqlValue> vs,ObInfo oi,BTree<long,object>m=null)
            : base(dp, _Mem(dp,vs,oi,m) + (_Columns,vs) + (Dependents, _Deps(vs)) 
                  + (Depth, 1 + _Depth(vs)))
        { }
        static BTree<long, object> _Mem(long dp, BList<SqlValue> vs, ObInfo oi, BTree<long, object> m)
        {
            m = m ?? BTree<long, object>.Empty;
            if (oi.Length != vs.Length)
                throw new DBException("22000");
            return m + (_Domain, oi.domain) + (Info, oi);
        }
        public static SqlRow operator+(SqlRow s,(long,object)m)
        {
            return new SqlRow(s.defpos, s.mem + m);
        }
        public static SqlRow operator+(SqlRow s,SqlValue v)
        {
            return new SqlRow(s.defpos, s.mem+(Info,s.info + new ObInfo(v.defpos,v.name,v.domain)));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlRow(defpos, m);
        }
        internal override SqlValue AddReg(Context cx, long rs, TRow key)
        {
            var nulls = true;
            for (var i = 0; i < columns.Length; i++)
                if (columns[i].AddReg(cx,rs,key) != null)
                    nulls = false;
            return nulls ? null : this;
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlRow(dp,mem);
        }
        internal SqlValue this[int i] => columns[i];
        internal SqlValue this[string n] => columns[info.map[n]??-1];
        internal SqlValue this[long p]
        {
            get
            {
                for (var b = columns.First(); b != null; b = b.Next())
                    if (b.value().defpos == p)
                        return b.value();
                return null;
            }
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = (SqlRow)base.Relocate(wr);
            var ss = BList<SqlValue>.Empty;
            var ch = false;
            for (var b=columns.First();b!=null;b=b.Next())
            {
                var s = (SqlValue)b.value().Relocate(wr);
                ss += s;
                ch = ch || (s != b.value());
            }
            if (ch)
                r = new SqlRow(defpos,ss,info);
            return r;
        }
        internal int Length => info.Length;
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = this;
            var cs = BList<SqlValue>.Empty;
            var ch = false;
            for (var b=columns.First();b!=null;b=b.Next())
            {
                var v = (SqlValue)b.value()._Replace(cx,so,sv);
                cs += (b.key(), v);
                if (v != b.value())
                    ch = true;
            }
            if (ch) // may also need a new ad-hoc ObInfo
                r = new SqlRow(defpos, cs, (info.defpos==defpos)?new ObInfo(defpos,cs):info);
            cx.done += (defpos, r);
            return r;
        }
        internal override (SqlValue,Query) Resolve(Context cx, Query fm)
        {
            var cs = BList<SqlValue>.Empty;
            var ch = false;
            for (var i=0;i<Length;i++)
            {
                var c = this[i];
                SqlValue v;
                (v,fm) = c.Resolve(cx, fm);
                cs += v;
                if (v != c)
                    ch = true;
            }
            var nr = ch ? (SqlValue)cx.Replace(this, new SqlRow(defpos, cs,
                new ObInfo(defpos, cs))) : this;
            return (nr,fm);
        }
        internal override DBObject Frame(Context cx)
        {
            var cs = BList<SqlValue>.Empty;
            for (var b = columns.First(); b != null; b = b.Next())
            {
                var v = (SqlValue)b.value().Frame(cx);
                if (v != b.value())
                    cs += v;
            }
            return cx.Add(new SqlRow(defpos,cs,info),true);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (SqlRow)base.AddFrom(cx, q);
            var vs = BList<SqlValue>.Empty;
            var ch = false;
            for (var b=r.columns.First();b!=null;b=b.Next())
            {
                var a = b.value().AddFrom(cx,q);
                if (a != b.value())
                    ch = true;
                vs += a;
            }
            if (ch)
                r += (_Columns, vs);
            return (SqlValue)cx.Add(r);
        }
        /// <summary>
        /// the value
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var vs = BTree<long,TypedValue>.Empty;
            for (int i = 0; i < info.Length; i++)
            {
                var s = columns[i];
                vs += (s.defpos, s.Eval(cx));
            }
            return new TRow(info.domain, vs);
        }
        internal override bool aggregates()
        {
            for (var i = 0; i < info.Length; i++)
                if (columns[i].aggregates())
                    return true;
            return false;
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            for (var i = 0; i < info.Length; i++)
                this[i].Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx,RowSet rs)
        {
            for (var i = 0; i < info.Length; i++)
                this[i].StartCounter(_cx,rs);
        }
        internal override void _AddIn(Context _cx,Cursor rb, TRow key, 
            ref BTree<long, bool?> aggsDone)
        {
            for (var i = 0; i < info.Length; i++)
                this[i]._AddIn(_cx,rb, key, ref aggsDone);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            for (var b = columns.First(); b != null; b = b.Next())
                qn = b.value().Needs(qn);
            return qn;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            for (var i=0;i<Length;i++)
            {
                var cm = "(";
                var c = this[i];
                sb.Append(cm); cm = ",";
                sb.Append(Uid(c.defpos) + "=");sb.Append(c);
                sb.Append(")");
            }
            return sb.ToString();
        }
    }
    internal class SqlRowArray : SqlValue
    {
        internal static readonly long
            Rows = -319; // BList<SqlRow>
        internal BList<SqlRow> rows =>
            (BList<SqlRow>)mem[Rows]?? BList<SqlRow>.Empty;
        public SqlRowArray(long dp, ObInfo oi, BList<SqlRow> rs) : base(dp, BTree<long, object>.Empty
             + (_Domain,new Domain(dp,Sqlx.ARRAY,oi.domain))+(Rows, rs))
        { }
        internal SqlRowArray(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlRowArray operator+(SqlRowArray s,(long,object)x)
        {
            return new SqlRowArray(s.defpos, s.mem + x);
        }
        public static SqlRowArray operator+(SqlRowArray s,SqlRow x)
        {
            return new SqlRowArray(s.defpos, s.mem + (Rows, s.rows + x));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlRowArray(defpos, m);
        }
        internal override SqlValue AddReg(Context cx, long rs, TRow key)
        {
            var nulls = true;
            for (var i = 0; i < rows.Length; i++)
                if (rows[i].AddReg(cx,rs,key) != null)
                    nulls = false;
            return nulls ? null : this;
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlRowArray(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var rs = BList<SqlRow>.Empty;
            var ch = false;
            for (var b=rows?.First();b!=null;b=b.Next())
            {
                var rw = (SqlRow)b.value().Relocate(wr);
                ch = ch || rw != b.value();
                rs += rw;
            }
            if (ch)
                r += (Rows, rs);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlRowArray)base._Replace(cx,so,sv);
            var rws = r.rows;
            for (var b=r.rows?.First();b!=null;b=b.Next())
            {
                var v = (SqlValue)b.value()._Replace(cx,so,sv);
                if (v != b.value())
                    rws += (b.key(), (SqlRow)v);
            }
            if (rws != r.rows)
                r += (Rows, rws);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SqlRowArray)base.Frame(cx);
            var rws = r.rows;
            for (var b = r.rows?.First(); b != null; b = b.Next())
            {
                var v = (SqlValue)b.value().Frame(cx);
                if (v != b.value())
                    rws += (b.key(), (SqlRow)v);
            }
            if (rws != r.rows)
                r += (Rows, rws);
            return cx.Add(r,true);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (SqlRowArray)base.AddFrom(cx, q);
            var rws = BList<SqlRow>.Empty;
            var ch = false;
            for (var b=r.rows?.First();b!=null;b=b.Next())
            {
                var a = (SqlRow)b.value().AddFrom(cx, q);
                if (a != b.value())
                    ch = true;
                rws += a;
            }
            if (ch)
                r += (Rows, rws);
            return (SqlValue)cx.Add(r);
        }
        internal override bool Uses(long t)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                if (b.value().Uses(t))
                    return true;
            return false;
        }
        internal override TypedValue Eval(Context cx)
        {
            var r = new TArray(domain, (int)rows.Count);
            for (var j = 0; j < rows.Count; j++)
                r[j] = rows[j].Eval(cx);
            return r;
        }
        internal override RowSet RowSet(long dp,Context cx,ObInfo rt)
        {
            var rs = BList<(long,TRow)>.Empty;
            for (var b = rows.First(); b != null; b = b.Next())
            {
                var v = b.value();
                rs += (v.defpos,rt.Eval(cx,v));
            }
            return new ExplicitRowSet(dp,cx, rt, rs);
        }
        internal override bool aggregates()
        {
            for (var i = 0; i < rows.Count; i++)
                if (rows[i].aggregates())
                    return true;
            return false;
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            for (var i = 0; i < rows.Count; i++)
                rows[i].Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx,RowSet rs)
        {
            for (var i = 0; i < rows.Count; i++)
                rows[i].StartCounter(_cx,rs);
        }
        internal override void _AddIn(Context _cx,Cursor rb, TRow key,
            ref BTree<long, bool?> aggsDone)
        {
            for (var i = 0; i < rows.Count; i++)
                rows[i]._AddIn(_cx,rb, key, ref aggsDone);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                qn = b.value().Needs(qn);
            return qn;
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            var cm = "";
            for (var b=rows.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.value());
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// This class is only used during aggregation computations
    /// </summary>
    internal class GroupRow : SqlRow
    {
        internal const long
            GroupInfo = -320, // BList<SqlValue>
            GroupMap = -261;  // BTree<SqlValue,int>
        public BList<SqlValue> groupInfo =>
            (BList<SqlValue>)mem[GroupInfo] ?? BList<SqlValue>.Empty;
        public BTree<SqlValue, int> groupMap =>
            (BTree<SqlValue, int>)mem[GroupMap] ?? BTree<SqlValue, int>.Empty;
        internal GroupRow(Context _cx, long dp, ObInfo oi, GroupingRowSet.GroupInfo gi,
            GroupingRowSet.GroupingBuilding gb, ObInfo gt, TRow key)
            : base(dp, _Columns(_cx,gi,gb, key),oi,BTree<long,object>.Empty
                  + (GroupMap, gi.group.members) + (Info, gt))
        { }
        static BList<SqlValue> _Columns(Context _cx, GroupingRowSet.GroupInfo gi,
            GroupingRowSet.GroupingBuilding gb,TRow key)
        {
            var dt = gb._grs.dataType;
            var oi = gb._grs.info;
            var columns = BList<SqlValue>.Empty;
            for (int j = 0; j < dt.Length; j++) // not grs.rowType!
            {
                var ci = oi.columns[j];
                var sc = (SqlValue)_cx.obs[ci.defpos];
                var v = key[ci.defpos];
                columns += (v is TypedValue tv) ? new SqlLiteral(ci.defpos,tv)
                        : sc.AddReg(_cx,gb._rowsetpos,key);
            }
            return columns;
        }
        /// <summary>
        /// the value
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var r = new TypedValue[info.Length];
            for (int i = 0; i < r.Length; i++)
                if (info.columns[i].domain.Coerce(this[i].Eval(cx)?.NotNull()) 
                    is TypedValue v)
                    r[i] = v;
                else
                    return null;
            return new TRow(info,r);
        }
    }
    internal class SqlXmlValue : SqlValue
    {
        internal const long
            Attrs = -323, // BTree<int,(XmlName,SqlValue)>
            Children = -324, // BList<SqlXmlValue>
            Content = -325, // SqlValue
            Element = -326; // XmlName
        public XmlName element => (XmlName)mem[Element];
        public BList<(XmlName, SqlValue)> attrs =>
            (BList<(XmlName, SqlValue)>)mem[Attrs] ?? BList<(XmlName, SqlValue)>.Empty;
        public BList<SqlXmlValue> children =>
            (BList<SqlXmlValue>)mem[Children]?? BList<SqlXmlValue>.Empty;
        public SqlValue content => (SqlValue)mem[Content]; // will become a string literal on evaluation
        public SqlXmlValue(long dp, XmlName n, SqlValue c, BTree<long, object> m) 
            : base(dp, m + (_Domain, Domain.XML)+(Element,n)+(Content,c)) { }
        internal SqlXmlValue(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlXmlValue operator+(SqlXmlValue s,(long,object)m)
        {
            return new SqlXmlValue(s.defpos, s.mem + m);
        }
        public static SqlXmlValue operator +(SqlXmlValue s, SqlXmlValue child)
        {
            return new SqlXmlValue(s.defpos, 
                s.mem + (Children,s.children+child));
        }
        public static SqlXmlValue operator +(SqlXmlValue s, (XmlName,SqlValue) attr)
        {
            return new SqlXmlValue(s.defpos,
                s.mem + (Attrs, s.attrs + attr));
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlXmlValue(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlXmlValue(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var d = wr.Fix(defpos);
            if (d != defpos)
                r = (SqlXmlValue)Relocate(d);
            var aa = BTree<int, (XmlName, SqlValue)>.Empty;
            var ch = false;
            for (var b=attrs?.First();b!=null;b=b.Next())
            {
                var a = (SqlValue)b.value().Item2.Relocate(wr);
                ch = ch || a != b.value().Item2;
                aa += (b.key(), (b.value().Item1, a));
            }
            if (ch)
                r += (Attrs, aa);
            var cs = BList<SqlXmlValue>.Empty;
            ch = false;
            for (var b=children?.First();b!=null;b=b.Next())
            {
                var c = (SqlXmlValue)b.value().Relocate(wr);
                ch = ch || c != b.value();
                cs += c;
            }
            if (ch)
                r += (Children, cs);
            var co = (SqlValue)content.Relocate(wr);
            if (co != content)
                r += (Content, co);
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
                var v = (SqlValue)b.value().Item2._Replace(cx,so,sv);
                if (v != b.value().Item2)
                    at += (b.key(), (b.value().Item1, v));
            }
            if (at != r.attrs)
                r += (Attrs, at);
            var co = (SqlValue)r.content._Replace(cx,so,sv);
            if (co != r.content)
                r += (Content, co);
            var ch = r.children;
            for(var b=ch?.First();b!=null;b=b.Next())
            {
                var v = (SqlValue)b.value()._Replace(cx,so,sv);
                if (v != b.value())
                    ch += (b.key(), (SqlXmlValue)v);
            }
            if (ch != r.children)
                r += (Children, ch);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SqlXmlValue)base.Frame(cx);
            var at = r.attrs;
            for (var b = at?.First(); b != null; b = b.Next())
            {
                var v = (SqlValue)b.value().Item2.Frame(cx);
                if (v != b.value().Item2)
                    at += (b.key(), (b.value().Item1, v));
            }
            if (at != r.attrs)
                r += (Attrs, at);
            var co = (SqlValue)r.content.Frame(cx);
            if (co != r.content)
                r += (Content, co);
            var ch = r.children;
            for (var b = ch?.First(); b != null; b = b.Next())
            {
                var v = (SqlValue)b.value().Frame(cx);
                if (v != b.value())
                    ch += (b.key(), (SqlXmlValue)v);
            }
            if (ch != r.children)
                r += (Children, ch);
            cx.done += (defpos, r);
            return cx.Add(r,true);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (SqlXmlValue)base.AddFrom(cx, q);
            var aa = r.attrs;
            for (var b=r.attrs.First();b!=null;b=b.Next())
            {
                var a = b.value().Item2.AddFrom(cx, q);
                if (a != b.value().Item2)
                    aa += (b.key(), (b.value().Item1, a));
            }
            if (aa != r.attrs)
                r += (Attrs, aa);
            var ch = r.children;
            for (var b=r.children.First();b!=null;b=b.Next())
            {
                var a = (SqlXmlValue)b.value().AddFrom(cx, q);
                if (a != b.value())
                    ch += (b.key(), a);
            }
            if (ch != r.children)
                r += (Children, ch);
            var c = r.content.AddFrom(cx,q);
            if (c != r.content)
                r += (Content, c);
            return (SqlValue)cx.Add(r);
        }
        internal override TypedValue Eval(Context cx)
        {
            var r = new TXml(element.ToString());
            for(var b=attrs?.First();b!=null;b=b.Next())
                if (b.value().Item2.Eval(cx)?.NotNull() is TypedValue ta)
                    r.attributes+=(b.value().Item1.ToString(), ta);
            for(var b=children?.First();b!=null;b=b.Next())
                if (b.value().Eval(cx) is TypedValue tc)
                    r.children+=(TXml)tc;
            if (content?.Eval(cx)?.NotNull() is TypedValue tv)
                r.content= (tv as TChar)?.value;
            return r;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            qn = content.Needs(qn);
            for (var b = attrs.First(); b != null; b = b.Next())
                qn = b.value().Item2.Needs(qn);
            for (var b = children.First(); b != null; b = b.Next())
                qn = b.value().Needs(qn);
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
                sb.Append(b.value().Item2);
            }
            if (content != null || children.Count!=0)
            {
                sb.Append(">");
                if (content != null)
                    sb.Append(content);
                else
                    for (var b=children.First(); b!=null;b=b.Next())
                        sb.Append(b.value());
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
            ArrayValuedQE = -327; // QueryExpression
        public QueryExpression aqe => (QueryExpression)mem[ArrayValuedQE];
        public SqlSelectArray(long dp, QueryExpression qe, BTree<long, object> m = null)
            : base(dp, (m ?? BTree<long, object>.Empty
                  + (_Domain, qe.domain) + (ArrayValuedQE, qe))) { }
        protected SqlSelectArray(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlSelectArray operator+(SqlSelectArray s,(long,object)x)
        {
            return new SqlSelectArray(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlSelectArray(defpos, m);
        }
        internal override SqlValue AddReg(Context cx, long rs, TRow key)
        {
            aqe.AddReg(cx,rs,key);
            return this;
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlSelectArray(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var aq = (QueryExpression)aqe.Relocate(wr);
            if (aq != aqe)
                r += (ArrayValuedQE, aq);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlSelectArray)base._Replace(cx,so,sv);
            var ae = r.aqe._Replace(cx,so,sv);
            if (ae != r.aqe)
                r += (ArrayValuedQE, ae);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SqlSelectArray)base.Frame(cx);
            var ae = r.aqe.Frame(cx);
            if (ae != r.aqe)
                r += (ArrayValuedQE, ae);
            cx.done += (defpos, r);
            return cx.Add(r,true);
        }
        internal override bool Uses(long t)
        {
            return aqe.Uses(t);
        }
        internal override TypedValue Eval(Context cx)
        {
            var va = new TArray(domain);
            var ars = aqe.RowSets(cx);
            var et = domain.elType;
            int j = 0;
            var nm = aqe.name;
            for (var rb=ars.First(cx);rb!= null;rb=rb.Next(cx))
            {
                var rw = rb;
                if (et==null)
                    va[j++] = rw[nm];
                else
                {
                    var vs = new TypedValue[aqe.display];
                    for (var i = 0; i < aqe.display; i++)
                        vs[i] = rw[i];
                    va[j++] = new TRow(aqe.rowType.info, vs);
                }
            }
            return va;
        }
        internal override bool aggregates()
        {
            return aqe.aggregates();
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            aqe.Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx,RowSet rs)
        {
            aqe.StartCounter(_cx,rs);
        }
        internal override void _AddIn(Context _cx,Cursor rb, TRow key,
            ref BTree<long, bool?> aggsDone)
        {
            aqe.AddIn(_cx,rb,key);
        }
        /// <summary>
        /// We aren't a column reference. If there are needs from where etc
        /// From will add them to cx.needed
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
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
    /// </summary>
    internal class SqlValueArray : SqlValue
    {
        internal const long
            Array = -328, // BList<SqlValue>
            Svs = -329; // SqlValueSelect
        /// <summary>
        /// the array
        /// </summary>
        public BList<SqlValue> array =>(BList<SqlValue>)mem[Array]??BList<SqlValue>.Empty;
        // alternatively, the source
        public SqlValueSelect svs => (SqlValueSelect)mem[Svs];
        /// <summary>
        /// construct an SqlArray value
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="a">the array</param>
        public SqlValueArray(long dp,BList<SqlValue> v)
            : base(dp, BTree<long,object>.Empty+(Array,v))
        { }
        public SqlValueArray(long dp, Domain t, SqlValueSelect sv)
            : base(dp, BTree<long, object>.Empty
                 + (_Domain, t) + (Svs, sv))
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
        internal override SqlValue AddReg(Context cx, long rs, TRow key)
        {
            var nulls = true;
            if (array != null)
                for (var i = 0; i < array.Length; i++)
                    if (array[i].AddReg(cx,rs,key) != null)
                        nulls = false;
            if (svs?.AddReg(cx,rs,key) != null)
                nulls = false;
            return nulls ? null : this;
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlValueArray(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = (SqlValueArray)base.Relocate(wr);
            var aa = BList<SqlValue>.Empty;
            var ch = false;
            for (var b=array?.First();b!=null;b=b.Next())
            {
                var a = (SqlValue)b.value().Relocate(wr);
                ch = ch || (a != b.value());
                aa += a;
            }
            if (ch)
                r += (Array, aa);
            var sv = (SqlValueSelect)svs?.Relocate(wr);
            if (sv != svs)
                r += (Svs, sv);
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
                var v = (SqlValue)b.value()._Replace(cx,so,sv);
                if (v != b.value())
                    ar += (b.key(), v);
            }
            if (ar != r.array)
                r += (Array, ar);
            var ss = (SqlValue)r.svs?._Replace(cx,so,sv);
            if (ss != r.svs)
                r += (Svs, ss);
            cx.done += (defpos, r);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (SqlValueArray)base.AddFrom(cx, q);
            var ar = BList<SqlValue>.Empty;
            var ch = false;
            for (var b=array.First();b!=null;b=b.Next())
            {
                var a = b.value().AddFrom(cx, q);
                if (a != b.value())
                    ch = true;
                ar += a;
            }
            if (ch)
                r += (Array, ar);
            var s = svs?.AddFrom(cx, q);
            if (s != svs)
                r += (Svs,s);
            return (SqlValue)cx.Add(r);
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SqlValueArray)base.Frame(cx);
            var ar = r.array;
            for (var b = ar?.First(); b != null; b = b.Next())
            {
                var v = (SqlValue)b.value().Frame(cx);
                if (v != b.value())
                    ar += (b.key(), v);
            }
            if (ar != r.array)
                r += (Array, ar);
            var ss = (SqlValue)r.svs?.Frame(cx);
            if (ss != r.svs)
                r += (Svs, ss);
            return cx.Add(r,true);
        }
        internal override bool Uses(long t)
        {
            for (var b = array?.First(); b != null; b = b.Next())
                if (b.value().Uses(t))
                    return true;
            return svs?.Uses(t)==true;
        }
        /// <summary>
        /// evaluate the array
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (svs != null)
            {
                var ar = new List<TypedValue>();
                var ers = svs?.Eval(cx) as RowSet;
                for (var b = ers.First(cx); b != null; b = b.Next(cx))
                    ar.Add(b[0]);
                return new TArray(domain, ar);
            }
            var a = new TArray(domain,(int)array.Count);
            for (int j = 0; j < (array?.Count??0); j++)
                a[j] = array[j]?.Eval(cx)?.NotNull() ?? domain.defaultValue;
            return a;
        }
        internal override bool aggregates()
        {
            if (array != null)
                for (var i = 0; i < array.Count; i++)
                    if (array[i].aggregates())
                        return true;
            return svs?.aggregates() ?? false;
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            if (array != null)
                for (var i = 0; i < array.Count; i++)
                    array[i].Build(_cx,rs);
            svs?.Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx, RowSet rs)
        {
            if (array != null)
                for (var i = 0; i < array.Count; i++)
                    array[i].StartCounter(_cx, rs);
            svs?.StartCounter(_cx,rs);
        }
        internal override void _AddIn(Context _cx,Cursor rb, TRow key,
            ref BTree<long, bool?> aggsDone)
        {
            if (array != null)
                for (var i = 0; i < array.Count; i++)
                    array[i]._AddIn(_cx,rb,key,ref aggsDone);
            svs?.AddIn(_cx,rb,key);
            base._AddIn(_cx,rb, key, ref aggsDone);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            for (var b = array.First(); b != null; b = b.Next())
                qn = b.value().Needs(qn);
            qn = svs.Needs(qn);
            return qn;
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
            Expr = -330, // Query
            Source = -331; // string
        /// <summary>
        /// the subquery
        /// </summary>
        public Query expr =>(Query)mem[Expr];
 //       public Domain targetType => (Domain)mem[TargetType];
        public string source => (string)mem[Source];
        public SqlValueSelect(long dp, Query q, string s)
            : base(dp,BTree<long,object>.Empty
                  +(Expr,q)+(Source,s)+(Info,q.rowType.info))
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
        internal override SqlValue AddReg(Context cx, long rs, TRow key)
        {
            expr.AddReg(cx,rs,key);
            return this;
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlValueSelect(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = (SqlValueSelect)base.Relocate(wr);
            var e = (Query)expr.Relocate(wr);
            if (e != expr)
                r += (Expr, e);
  //          var dt = (Domain)targetType.Relocate(wr);
  //          if (dt != targetType)
  //              r += (TargetType, dt);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlValueSelect)base._Replace(cx,so,sv);
            var ex = r.expr._Replace(cx,so,sv);
            if (ex != r.expr)
                r += (Expr, ex);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlValueSelect)base.Frame(cx);
            var ex = r.expr.Frame(cx);
            if (ex != r.expr)
                r += (Expr, ex);
            return cx.Add(r,true);
        }
        internal override bool Uses(long t)
        {
            return expr.Uses(t);
        }
        internal override Query Conditions(Context cx, Query q, bool disj,out bool move)
        {
            move = false;
            return expr.Conditions(cx);
        }
        internal override TypedValue Eval(Context cx)
        {
            var ers = expr.RowSets(cx);
            if (kind == Sqlx.TABLE)
                return ers;
            var rb = ers.First(cx);
            if (rb == null)
                return domain.defaultValue;
            TypedValue tv = rb[0]; // rb._rs.qry.rowType.columns[0].Eval(cx)?.NotNull();
   //        if (targetType != null)
   //             tv = targetType.Coerce(tv);
            return tv;
        }
        internal override bool aggregates()
        {
            return expr.aggregates();
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            expr.Build(_cx,rs);
        }
        internal override RowSet RowSet(long dp,Context cx,ObInfo rt)
        {
            var r = expr.RowSets(cx);
            cx.data += (dp, r);
            return r;
        }
        internal override void StartCounter(Context _cx, RowSet rs)
        {
            expr.StartCounter(_cx,rs);
        }
        internal override void _AddIn(Context _cx,Cursor rb, TRow key, 
            ref BTree<long, bool?> aggsDone)
        {
            expr.AddIn(_cx,rb,key);
        }
        /// <summary>
        /// We aren't a column reference. If there are needs from e.g.
        /// where conditions From will add them to cx.needed
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            return qn;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
   //         sb.Append(" TargetType: ");sb.Append(targetType);
            sb.Append(" (");sb.Append(expr); sb.Append(")");
            return sb.ToString();
        }
    }
    /// <summary>
    /// A Column Function SqlValue class
    /// </summary>
    internal class ColumnFunction : SqlValue
    {
        internal const long
            Bits = -333; // BList<long>
        /// <summary>
        /// the set of column references
        /// </summary>
        internal BList<long> bits => (BList<long>)mem[Bits];
        /// <summary>
        /// constructor: a new ColumnFunction
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="t">the datatype</param>
        /// <param name="c">the set of TableColumns</param>
        public ColumnFunction(long dp, BList<long> c)
            : base(dp, BTree<long, object>.Empty + (_Domain, Domain.Bool)+ (Bits, c)) { }
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
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var bs = BList<long>.Empty;
            var ch = false;
            for(var b = bits.First();b != null;b=b.Next())
            {
                var c = wr.Fix(b.value());
                ch = ch || c != b.value();
                bs += c;
            }
            if (ch)
                r += (Bits, bs);
            return r;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
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
            Spec = -334; // CursorSpecification
        internal CursorSpecification spec=>(CursorSpecification)mem[Spec];
        internal SqlCursor(long dp, CursorSpecification cs, string n) 
            : base(dp, BTree<long,object>.Empty+
                  (_Domain,cs.domain)+(Name, n)+(Spec,cs)
                  +(Dependents,new BTree<long,bool>(cs.defpos,true))
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (SqlCursor)base.Relocate(wr);
            var sp = (CursorSpecification)spec.Relocate(wr);
            if (sp != spec)
                r += (Spec, sp);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlCursor)base._Replace(cx,so,sv);
            var sp = r.spec._Replace(cx,so,sv);
            if (sp != r.spec)
                r += (Spec, sp);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SqlCursor)base.Frame(cx);
            var sp = r.spec.Frame(cx);
            if (sp != r.spec)
                r += (Spec, sp);
            return cx.Add(r,true);
        }
        internal override bool Uses(long t)
        {
            return spec.Uses(t);
        }
        /// <summary>
        /// We aren't a column reference. If there are needs from e.g.
        /// where conditions From will add them to cx.needed 
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
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
    internal class SqlCall : SqlValue
    {
        internal const long
            Call = -335; // CallStatement
        public CallStatement call =>(CallStatement)mem[Call];
        public SqlCall(long dp, CallStatement c, BTree<long,object>m=null)
            : base(dp, m??BTree<long, object>.Empty 
                  + (Call, c)+(Dependents,new BTree<long,bool>(c.defpos,true))
                  +(Depth,1+c.depth)+(_Domain,c.domain))
        { }
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (SqlCall)base.Relocate(wr);
            var c = (CallStatement)call.Relocate(wr);
            if (c != call)
                r += (Call, c);
            return r;
        }
        internal override SqlValue AddReg(Context cx, long rs, TRow key)
        {
            var nulls = true;
            for (var i = 0; i < call.parms.Length; i++)
                if (call.parms[i].AddReg(cx,rs,key) != null)
                    nulls = false;
            return nulls ? null : this;
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (SqlCall)base.AddFrom(cx, q);
            var c = r.call;
            var a = c.var?.AddFrom(cx, q);
            if (a != c.var)
                c += (CallStatement.Var, a); 
            var vs = BList<SqlValue>.Empty;
            for (var i = 0; i < call.parms.Length; i++)
                vs += r.call.parms[i].AddFrom(cx, q);
            c += (CallStatement.Parms, vs);
            r += (Call,c);
            return (SqlValue)cx.Add(r);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlCall)base._Replace(cx,so,sv);
            var ca = r.call._Replace(cx,so,sv);
            if (ca != r.call)
                r += (Call, ca);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SqlCall)base.Frame(cx);
            var c = (CallStatement)call.Frame(cx);
            if (c != call)
                r += (Call, c);
            return cx.Add(r,true);
        }
        internal override bool Uses(long t)
        {
           return call.procdefpos==t;
        }
        internal override bool aggregates()
        {
            for (var i = 0; i < call.parms.Count; i++)
                if (call.parms[i].aggregates())
                    return true;
            return false;
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            var mp = rs.info.map;
            for (var i = 0; i < call.parms.Count; i++)
                if (mp.Contains((call.parms[i]).name))
                    call.parms[i].Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx, RowSet rs)
        {
            var mp = rs.info.map;
            for (var i = 0; i < call.parms.Count; i++)
                if (mp.Contains((call.parms[i]).name))
                    call.parms[i].StartCounter(_cx, rs);
        }
        internal override void _AddIn(Context _cx,Cursor rb, TRow key, ref BTree<long, bool?> aggsDone)
        {
            var mp = rb._info.map;
            for (var i = 0; i < call.parms.Count; i++)
                if (mp.Contains(call.parms[i].name))
                    call.parms[i]._AddIn(_cx,rb, key, ref aggsDone);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            for (var b = call.parms.First(); b != null; b = b.Next())
                qn = b.value().Needs(qn);
            return qn;
        }
        public override string ToString()
        {
            var sb = new StringBuilder("call ");
            sb.Append(call);
            return sb.ToString();
        }
    }
    /// <summary>
    /// An SqlValue that is a procedure/function call or static method
    /// </summary>
    internal class SqlProcedureCall : SqlCall
    {
        public SqlProcedureCall(long dp, CallStatement c) : base(dp, c) { }
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (SqlProcedureCall)base.Relocate(wr);
            var c = (CallStatement)call.Relocate(wr);
            if (c != call)
                r += (Call, c);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlProcedureCall)base._Replace(cx, so, sv);
            var ca = r.call._Replace(cx, so, sv);
            if (ca != r.call)
                r += (Call, ca);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SqlProcedureCall)base.Frame(cx);
            var c = (CallStatement)call.Frame(cx);
            if (c != call)
                r += (Call, c);
            return cx.Add(r,true);
        }
        /// <summary>
        /// evaluate the procedure call
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var tr = cx.db;
            try
            {
                var proc = (Procedure)tr.objects[call.procdefpos];
                proc.Exec(cx, call.parms);
                return cx.val??domain.defaultValue;
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
        internal override void Eqs(Context cx,ref Adapters eqs)
        {
            var proc = (Procedure)cx.db.objects[call.procdefpos];
            if (cx.db.objects[proc.inverse] is Procedure inv)
                eqs = eqs.Add(proc.defpos, call.parms[0].defpos, proc.defpos, inv.defpos);
            base.Eqs(cx,ref eqs);
        }
        internal override int ColFor(Context cx,Query q)
        {
            if (call.parms.Count == 1)
                return call.parms[0].ColFor(cx,q);
            return base.ColFor(cx,q);
        }
        public override string ToString()
        {
            return call.name + "(..)";
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
        public SqlMethodCall(long dp, CallStatement c) : base(dp,c)
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (SqlMethodCall)base.Relocate(wr);
            var c = (CallStatement)call.Relocate(wr);
            if (c != call)
                r += (Call, c);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlMethodCall)base._Replace(cx, so, sv);
            var ca = r.call._Replace(cx, so, sv);
            if (ca != r.call)
                r += (Call, ca);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SqlMethodCall)base.Frame(cx);
            var c = (CallStatement)call.Frame(cx);
            if (c != call)
                r += (Call, c);
            return cx.Add(r,true);
        }
        internal override (SqlValue,Query) Resolve(Context cx, Query fm)
        {
            SqlValue v;
            (v,fm) = base.Resolve(cx, fm);
            var mc = (SqlMethodCall)v;
            (v,fm) = call.var.Resolve(cx, fm);
            if (v != call.var)
            {
                var ut = v.domain;
                var p = v.methods[call.name]?[(int)call.parms.Count]??-1L;
                var nc = call + (CallStatement.Var, v) + (CallStatement.ProcDefPos, p);
                var pr = cx.db.objects[p] as Procedure;
                mc = mc + (Call, nc) + (_Domain, pr.retType);
            }
            return ((SqlValue)cx.Add(mc),fm);
        }
        /// <summary>
        /// Evaluate the method call and return the result
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var tr = cx.db;
            if (call.var == null)
                throw new PEException("PE241");
            var proc = (Method)tr.objects[call.procdefpos];
            return proc.Exec(cx, call.var, call.parms);
        }
        public override string ToString()
        {
            return call.name.ToString()+"(..)";
        }
    }
    /// <summary>
    /// An SqlValue that is a constructor expression
    /// </summary>
    internal class SqlConstructor : SqlCall
    {
        internal const long
            Sce = -336, //SqlRow
            Udt = -337; // Domain
        /// <summary>
        /// the type
        /// </summary>
        public Domain ut =>(Domain)mem[Udt];
        public SqlRow sce =>(SqlRow)mem[Sce];
        /// <summary>
        /// set up the Constructor SqlValue
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="u">the type</param>
        /// <param name="c">the call statement</param>
        public SqlConstructor(long dp, Domain u, CallStatement c)
            : base(dp, c,new BTree<long,object>(Udt,u))
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (SqlConstructor)base.Relocate(wr);
            var s = (SqlRow)sce.Relocate(wr);
            if (s != sce)
                r += (Sce, s);
            var u = (Domain)ut.Relocate(wr);
            if (u != ut)
                r += (Udt, u);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlConstructor)base._Replace(cx,so,sv);
            var sc = r.sce._Replace(cx,so,sv);
            if (sc != r.sce)
                r += (Sce, sc);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlConstructor)base.Frame(cx);
            var sc = r.sce.Frame(cx);
            if (sc != r.sce)
                r += (Sce, sc);
            cx.done += (defpos, r);
            return cx.Add(r,true);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (SqlConstructor)base.AddFrom(cx, q);
            var a = r.sce.AddFrom(cx, q);
            if (a != r.sce)
                r += (Sce, a);
            return (SqlValue)cx.Add(r);
        }
        internal override bool Uses(long t)
        {
            return ut.structure==t;
        }
        /// <summary>
        /// evaluate the constructor and return the new object
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var tr = cx.db;
            var proc = (Method)tr.objects[call.procdefpos];
            return proc.Exec(cx,null,call.parms);
        }
        public override string ToString()
        {
            return call.ToString();
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
        public Domain ut=>(Domain)mem[SqlConstructor.Udt];
        public SqlRow sce=>(SqlRow)mem[SqlConstructor.Sce];
        /// <summary>
        /// construct a SqlValue default constructor for a type
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="u">the type</param>
        /// <param name="lk">the actual parameters</param>
        public SqlDefaultConstructor(long dp, Domain u, BList<SqlValue> ins)
            : base(dp, BTree<long, object>.Empty+(SqlConstructor.Udt, u)
                  +(SqlConstructor.Sce,ins)
                  +(Dependents,_Deps(ins))+(Depth,1+_Depth(ins)))
        { }
        protected SqlDefaultConstructor(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlDefaultConstructor operator +(SqlDefaultConstructor s, (long, object) x)
        {
            return new SqlDefaultConstructor(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlDefaultConstructor(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlDefaultConstructor(dp,mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlDefaultConstructor)base._Replace(cx,so,sv);
            var sc = r.sce._Replace(cx,so,sv);
            if (sc != r.sce)
                r += (SqlConstructor.Sce, sc);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SqlDefaultConstructor)base.Frame(cx);
            var sc = r.sce.Frame(cx);
            if (sc != r.sce)
                r += (SqlConstructor.Sce, sc);
            return cx.Add(r,true);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (SqlDefaultConstructor)base.AddFrom(cx, q);
            var a = r.sce.AddFrom(cx, q);
            if (a != r.sce)
                r += (SqlConstructor.Sce, a);
            return (SqlValue)cx.Add(r);
        }
        internal override bool Uses(long t)
        {
            return ut.structure ==t;
        }
        /// <summary>
        /// Evaluate the default constructor
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var tr = cx.db;
            var dt = ut;
            try
            { //??
                var oi = tr.role.obinfos[dt.defpos] as ObInfo;
                var obs = BTree<long,TypedValue>.Empty;
                for (var b=oi.columns.First();b!=null;b=b.Next())
                    obs += (b.value().defpos,sce[b.key()].Eval(cx));
                return new TRow(ut,obs);
            }
            catch (DBException e)
            {
                throw e;
            }
            catch (Exception)
            {
                return dt.defaultValue;
            }
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            return sce.Needs(qn);
        }
        public override string ToString()
        {
            return sce.name+"(..)";
        }
     }
    /// <summary>
    /// A built-in SQL function
    /// </summary>
    internal class SqlFunction : SqlValue
    {
        internal const long
            Filter = -338, //SqlValue
            Mod = -340, // Sqlx
            Monotonic = -341, // bool
            Op1 = -342, // SqlValue
            Op2 = -343, // SqlValue
            Query = -344,//Query
            _Val = -345,//SqlValue
            Window = -346, // WindowSpecification
            WindowId = -347; // long
        /// <summary>
        /// the query
        /// </summary>
        internal Query query => (Query)mem[Query];
        public override Sqlx kind => (Sqlx)mem[Kind];
        /// <summary>
        /// A modifier for the function from the parser
        /// </summary>
        public Sqlx mod => (Sqlx)mem[Mod];
        /// <summary>
        /// the value parameter for the function
        /// </summary>
        public SqlValue val => (SqlValue)mem[_Val];
        /// <summary>
        /// operands for the function
        /// </summary>
        public SqlValue op1 => (SqlValue)mem[Op1];
        public SqlValue op2 => (SqlValue)mem[Op2];
        /// <summary>
        /// a Filter for the function
        /// </summary>
        public SqlValue filter => (SqlValue)mem[Filter];
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
        public SqlFunction(long dp, Sqlx f, SqlValue vl, SqlValue o1, SqlValue o2, Sqlx m,
            BTree<long, object> mm = null) :
            base(dp, (mm ?? BTree<long, object>.Empty) + (_Domain, _Type(f, vl, o1))
                + (Info, new ObInfo(dp,(mm?[_Alias]??f).ToString(),_Type(f,vl,o1)))
                +(Name,f.ToString())+(Kind,f)+(Mod,m)+(_Val,vl)+(Op1,o1)+(Op2,o2)
                +(Dependents,_Deps(vl,o1,o2)) +(Depth,_Depth(vl,o1,o2)))
        { }
        protected SqlFunction(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlFunction operator+(SqlFunction s,(long,object)x)
        {
            var m = s.mem + x;
            var (a, v) = x;
            if (a == Op1)
                m += (_Domain, _Type(s.kind, s.val, (SqlValue)v));
            if (a == _Val)
                m += (_Domain, _Type(s.kind, (SqlValue)v, s.op1));
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
        internal override SqlValue AddReg(Context cx,long rs,TRow key)
        {
            cx.from += (defpos, rs);
            var fd = cx.func[defpos];
            if (fd == null)
                cx.func += (defpos, fd = new FunctionData(key.dataType.kind));
               //     (key.dataType.kind));
            if ((window != null || aggregates0()))
            {
                var cur = fd.regs?[key];
                if (cur == null)
                {
                    cur = new FunctionData.Register() { profile = key };
                    if (fd.regs == null)
                        fd.regs = new CTree<TRow, FunctionData.Register>(key.dataType.kind);
                    cx.func[defpos].regs+=(key, cur);
                }
                fd.cur = cur;
            }
            else
            {
                val.AddReg(cx,rs,key);
                op1.AddReg(cx,rs,key);
                op2.AddReg(cx,rs,key);
            }
            return this;
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var f = (SqlValue)filter?.Relocate(wr);
            if (f != filter)
                r += (Filter, f);
            var op = (SqlValue)op1?.Relocate(wr);
            if (op != op1)
                r += (Op1, op);
            op = (SqlValue)op2?.Relocate(wr);
            if (op != op2)
                r += (Op2, op);
            var v = (SqlValue)val?.Relocate(wr);
            if (v != val)
                r += (_Val, v);
            var w = (WindowSpecification)window?.Relocate(wr);
            if (w != window)
                r += (Window, w);
            var wi = wr.Fix(windowId);
            if (wi != windowId)
                r += (WindowId, wi);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (SqlFunction)base.AddFrom(cx, q);
            var a = r.val?.AddFrom(cx, q);
            if (a != r.val)
                r += (_Val, a); 
            a = r.op1?.AddFrom(cx, q);
            if (a != r.op1)
                r += (Op1, a); 
            a = r.op2?.AddFrom(cx, q);
            if (a != r.op2)
                r += (Op2, a); 
            return (SqlValue)cx.Add(r);
        }
        static BTree<long,bool> _Deps(SqlValue vl,SqlValue o1,SqlValue o2)
        {
            var r = BTree<long, bool>.Empty;
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
        internal override (SqlValue,Query) Resolve(Context cx, Query fm)
        {
            SqlValue vl = null, o1 = null, o2 = null;
            (vl,fm) = val?.Resolve(cx, fm)??(vl,fm);
            (o1,fm) = op1?.Resolve(cx, fm)??(o1,fm);
            (o2,fm) = op2?.Resolve(cx, fm)??(o2,fm);
            if (vl != val || o1 != op1 || o2 != op2)
                return ((SqlValue)cx.Replace(this,
                    new SqlFunction(defpos, kind, vl, o1, o2, mod, mem)),fm);
            return (this,fm);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlFunction)base._Replace(cx,so,sv);
            var fi = r.filter?._Replace(cx,so,sv);
            if (fi != r.filter)
                r += (Filter, fi);
            var o1 = r.op1?._Replace(cx,so,sv);
            if (o1 != r.op1)
                r += (Op1, o1);
            var o2 = r.op2?._Replace(cx,so,sv);
            if (o2 != r.op2)
                r += (Op2, o2);
            var vl = r.val?._Replace(cx,so,sv);
            if (vl != r.val)
                r += (_Val, vl);
            var qr = r.query?._Replace(cx, so, sv);
            if (qr != r.query)
                r += (Query, qr);
            cx.done += (defpos, r);
            return cx.Add(r);
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SqlFunction)base.Frame(cx);
            var fi = r.filter?.Frame(cx);
            if (fi != r.filter)
                r += (Filter, fi);
            var o1 = r.op1?.Frame(cx);
            if (o1 != r.op1)
                r += (Op1, o1);
            var o2 = r.op2?.Frame(cx);
            if (o2 != r.op2)
                r += (Op2, o2);
            var vl = r.val?.Frame(cx);
            if (vl != r.val)
                r += (_Val, vl);
            var qr = r.query?.Frame(cx);
            if (qr != r.query)
                r += (Query, qr);
            return cx.Add(r,true);
        }
        internal override SqlValue Operand()
        {
            if (aggregates0())
                return val;
            return val?.Operand();
        }
        /// <summary>
        /// Prepare Window Function evaluation
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="q"></param>
        internal SqlFunction RowSets(Query q)
        {
            // we first compute the needs of this window function
            // The key for registers will consist of partition/grouping columns
            // Each register has a rowset ordered by the order columns if any
            // for the moment we just use the whole source row
            // We build all of the WRS's at this stage for saving in f
            return this+(Window,window
            +(WindowSpecification.PartitionType,new Domain(window.defpos,Sqlx.ROW,BTree<long,object>.Empty)));
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            if (window == null || _cx.func.Contains(defpos))
                return;
            var fd = new FunctionData();
            _cx.func += (defpos, fd);
            fd.building = true;
//           fd.regs = BTree<long,CTree<TRow, FunctionData.Register>>.Empty;
            var cx = new Context(_cx);
            for (var b = rs.First(cx);b!=null;b=b.Next(cx))
            {
                PRow ks = null;
                for (var i = window.partition - 1;i>=0; i--)
                    ks = new PRow(window.order.items[i].Eval(cx),ks);
                var rw = new TRow(window.partitionType.info, ks);
                ks = null;
                for (var i = (int)window.order.items.Length - 1; i >= window.partition; i--)
                    ks = new PRow(window.order.items[i].Eval(cx), ks);
                var okey = new TRow(window.partitionType.info, ks);
                var worder = OrderSpec.Empty;
                var its = BTree<int, DBObject>.Empty;
                for (var i = window.partition; i < window.order.items.Length; i++)
                    its+=((int)its.Count,window.order.items[i]);
                worder = worder+ (OrderSpec.Items, its);
                var fc = fd.regs[rw]; //[window.defpos]?[rw];
 //               if (fc ==null)
 //               {
 //                   var t = fd.regs[window.defpos] ?? new CTree<TRow,FunctionData.Register>(Sqlx.INT);
//                    t += (rw, fc = new FunctionData.Register());
 //                   fd.regs += (window.defpos, t);
 //                   fc.wrs = new RTree(rs,
//                        new TreeInfo(window.partitionType, TreeBehaviour.Allow, TreeBehaviour.Allow));
 //               }
                var dt = rs.info;
                var vs = new TypedValue[dt.Length];
                for (var i = 0; i < dt.Length; i++)
                    vs[i] = cx.obs[dt[i].defpos].Eval(cx);
                fc.wrs += (okey,new TRow(dt,vs));
            }
        }
        /// <summary>
        /// See if two current values match as expressions
        /// </summary>
        /// <param name="v">one SqlValue</param>
        /// <param name="w">another SqlValue</param>
        /// <returns>whether they match</returns>
        static bool MatchExp(Query q,SqlValue v, SqlValue w)
        {
            return v?.MatchExpr(q,w) ?? w == null;
        }
        /// <summary>
        /// Check SqlValues for structural matching
        /// </summary>
        /// <param name="v">another sqlValue</param>
        /// <returns>whether they match</returns>
        internal override bool _MatchExpr(Query q,SqlValue v)
        {
            return (v is SqlFunction f && (domain == null || domain == v.domain)) &&
             MatchExp(q,val, f.val) && MatchExp(q,op1, f.op1) && MatchExp(q,op2, f.op2);
        }
        internal override bool Grouped(GroupSpecification gs)
        {
            return base.Grouped(gs) || ((!aggregates0()) && val.Grouped(gs));
        }
        internal static Domain _Type(Sqlx kind,SqlValue val, SqlValue op1)
        {
            switch (kind)
            {
                case Sqlx.ABS: return val?.domain??Domain.UnionNumeric;
                case Sqlx.ANY: return Domain.Bool;
                case Sqlx.AVG: return Domain.UnionNumeric;
                case Sqlx.ARRAY: return Domain.Collection; 
                case Sqlx.CARDINALITY: return Domain.Int;
                case Sqlx.CASE: return val.domain;
                case Sqlx.CAST: return ((SqlTypeExpr)op1).type;
                case Sqlx.CEIL: return val?.domain ?? Domain.UnionNumeric;
                case Sqlx.CEILING: return val?.domain ?? Domain.UnionNumeric;
                case Sqlx.CHAR_LENGTH: return Domain.Int;
                case Sqlx.CHARACTER_LENGTH: return Domain.Int;
                case Sqlx.CHECK: return Domain.Char;
                case Sqlx.COLLECT: return Domain.Collection;
                case Sqlx.COUNT: return Domain.Int;
                case Sqlx.CURRENT: return Domain.Bool; // for syntax check: CURRENT OF
                case Sqlx.CURRENT_DATE: return Domain.Date;
                case Sqlx.CURRENT_TIME: return Domain.Timespan;
                case Sqlx.CURRENT_TIMESTAMP: return Domain.Timestamp;
                case Sqlx.ELEMENT: return val?.domain.elType?.domain??Domain.Content;
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
                case Sqlx.VERSIONING: return Domain.Int;
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
            FunctionData.Register fc = null;
            if (aggregates())
            {
                if (!cx.from.Contains(defpos))
                    throw new PEException("PE190");
                var rp = cx.from[defpos];
                if (cx.cursors[rp] is Cursor cs && (!(cs is GroupingRowSet.GroupingBuilding))
                    && cs.values.Contains(defpos))
                    return cs[defpos];
                var rs = cx.data[rp]
                    ?? throw new PEException("PE187");
                // building the value
                var fd = cx.func[defpos];
                if (fd == null)
                    cx.func += (defpos, fd = new FunctionData());
                fc = fd.cur; 
                if (fd == null)
                    cx.func += (defpos, fd = new FunctionData());
                if (window != null)
                {
                    if (fd.valueInProgress)
                        return null;
                    fd.valueInProgress = true;
                    PRow ks = null;
                    for (var i = window.partition - 1; i >= 0; i--)
                        ks = new PRow(window.order.items[i].Eval(cx), ks);
                    fc = fd.regs /*[rp] */[new TRow(window.partitionType.info, ks)];
                    fc.Clear();
                    ks = null;
                    for (var i = (int)window.order.items.Length - 1; i >= window.partition; i--)
                        ks = new PRow(window.order.items[i].Eval(cx), ks);
                    var dt = rs.info;
                    for (var b = firstTie = fc.wrs.PositionAt(cx, ks); b != null; b = b.Next(cx))
                    {
                        for (var i = 0; i < dt.Length; i++)
                        {
                            var c = dt[i];
                            var n = dt[i].name;
                            if (b[i] is TypedValue tv && c.domain.Compare(tv, b[i]) != 0)
                                goto skip;
                        }
                        fc.wrb = b;
                        break;
                    skip:;
                    }
                    for (RTreeBookmark b = fc.wrs.First(cx); b != null; b = b.Next(cx) as RTreeBookmark)
                        if (InWindow(cx, b, fc))
                            switch (window.exclude)
                            {
                                case Sqlx.NO:
                                    AddIn(cx, b);
                                    break;
                                case Sqlx.CURRENT:
                                    if (b._pos != fc.wrb._pos)
                                        AddIn(cx, b);
                                    break;
                                case Sqlx.TIES:
                                    if (!Ties(fc.wrb, b))
                                        AddIn(cx, b);
                                    break;
                            }
                    fd.valueInProgress = false;
                }
            }
            TypedValue v = null;
            switch (kind)
            {
                case Sqlx.ABS:
                    v = val?.Eval(cx)?.NotNull();
                    if (v == null)
                        return null;
                    switch (val.domain.kind)
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
                                var cs = val.domain.unionOf;
                                for (int i = 0; i < cs.Count; i++)
                                    if (cs[i].kind == Sqlx.INTEGER)
                                        goto case Sqlx.INTEGER;
                                for (int i = 0; i < cs.Count; i++)
                                    if (cs[i].kind == Sqlx.NUMERIC)
                                        goto case Sqlx.NUMERIC;
                                for (int i = 0; i < cs.Count; i++)
                                    if (cs[i].kind == Sqlx.REAL)
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
                        fc.acc = new TArray(new Domain(defpos,Sqlx.ARRAY, fc.mset.tree?.First()?.key().dataType));
                        var ar = fc.acc as TArray;
                        for (var d = fc.mset.tree.First();d!= null;d=d.Next())
                            ar.list.Add(d.key());
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
                        v = val?.Eval(cx)?.NotNull();
                        if (v == null)
                            return null;
                        if (!(v.dataType.kind != Sqlx.MULTISET))
                            throw new DBException("42113", v).Mix();
                        var m = (TMultiset)v;
                        return new TInt(m.Count);
                    }
                case Sqlx.CASE:
                    {
                        v = val?.Eval(cx)?.NotNull();
                        SqlFunction f = this;
                        for (; ; )
                        {
                            SqlFunction fg = f.op2 as SqlFunction;
                            if (fg == null)
                                return f.op2?.Eval(cx)??null;
                            if (f.op1.domain.Compare(f.op1.Eval(cx), v) == 0)
                                return f.val.Eval(cx);
                            f = fg;
                        }
                    }
                case Sqlx.CAST:
                    {
                        v = val?.Eval(cx)?.NotNull();
                        if (v == null)
                            return null;
                        return domain.Coerce(v);
                    }
                case Sqlx.CEIL: goto case Sqlx.CEILING;
                case Sqlx.CEILING:
                    v = val?.Eval(cx)?.NotNull();
                    if (v == null)
                        return null;
                    switch (val.domain.kind)
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
                        v = val?.Eval(cx)?.NotNull();
                        if (v == null)
                            return domain.defaultValue;
                        if (v?.ToString().ToCharArray() is char[] chars)
                            return new TInt(chars.Length);
                        return new TInt(0);
                    }
                case Sqlx.CHARACTER_LENGTH: goto case Sqlx.CHAR_LENGTH;
             //   case Sqlx.CHECK: return new TRvv(rb);
                case Sqlx.COLLECT: return domain.Coerce((TypedValue)fc.mset ??TNull.Value);
                //		case Sqlx.CONVERT: transcoding all seems to be implementation-defined TBD
                case Sqlx.COUNT: return new TInt(fc.count);
                case Sqlx.CURRENT:
                    {
                        if (val.Eval(cx) is Cursor tc && cx.values[tc._rowsetpos] is Cursor tq)
                            return TBool.For(tc._pos == tq._pos);
                        break;
                    }
                case Sqlx.CURRENT_DATE: return new TDateTime(Domain.Date, DateTime.UtcNow);
                case Sqlx.CURRENT_ROLE: return new TChar(cx.db.role.name);
                case Sqlx.CURRENT_TIME: return new TDateTime(Domain.Timespan, DateTime.UtcNow);
                case Sqlx.CURRENT_TIMESTAMP: return new TDateTime(Domain.Timestamp, DateTime.UtcNow);
                case Sqlx.CURRENT_USER: return new TChar(cx.db.user.name);
                case Sqlx.ELEMENT:
                    {
                        v = val?.Eval(cx)?.NotNull();
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
                    v = val?.Eval(cx)?.NotNull();
                    if (v == null)
                        return null;
                    if (v == TNull.Value)
                        return domain.defaultValue;
                    return new TReal(Math.Exp(v.ToDouble()));
                case Sqlx.EVERY:
                    {
                        object o = fc.mset.tree[TBool.False];
                        return (o == null || ((int)o) == 0) ? TBool.True : TBool.False;
                    }
                case Sqlx.EXTRACT:
                    {
                        v = val?.Eval(cx)?.NotNull();
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
                    v = val?.Eval(cx)?.NotNull();
                    if (v == null)
                        return null;
                    if (v.Val() == null)
                        return v;
                    switch (val.domain.kind)
                    {
                        case Sqlx.INTEGER:
                            return v;
                        case Sqlx.DOUBLE:
                            return new TReal(Math.Floor(v.ToDouble()));
                        case Sqlx.NUMERIC:
                            return new TNumeric(Common.Numeric.Floor((Common.Numeric)v.Val()));
                    }
                    break;
                case Sqlx.FUSION: return domain.Coerce(fc.mset);
                case Sqlx.INTERSECTION:return domain.Coerce(fc.mset);
                case Sqlx.LAST: return fc.mset.tree.Last().key();
                case Sqlx.LN:
                    v = val?.Eval(cx)?.NotNull();
                    if (v == null)
                        return null;
                    if (v.Val() == null)
                        return v;
                    return new TReal(Math.Log(v.ToDouble()));
                case Sqlx.LOCALTIME: return new TDateTime(Domain.Date, DateTime.Now);
                case Sqlx.LOCALTIMESTAMP: return new TDateTime(Domain.Timestamp, DateTime.Now);
                case Sqlx.LOWER:
                    {
                        v = val?.Eval(cx)?.NotNull();
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
                    if (op1 != null)
                        v = op1.Eval(cx);
                    if (v.Val() == null)
                        return domain.defaultValue;
                    switch (op1.domain.kind)
                    {
                        case Sqlx.INTEGER:
                            return new TInt(v.ToLong() % op2.Eval(cx).ToLong());
                        case Sqlx.NUMERIC:
                            return new TNumeric(((Numeric)v.Val()) % (Numeric)op2.Eval(cx).Val());
                    }
                    break;
                case Sqlx.NORMALIZE:
                    if (val != null)
                        v = val.Eval(cx);
                    return v; //TBD
                case Sqlx.NULLIF:
                    {
                        TypedValue a = op1.Eval(cx)?.NotNull();
                        if (a == null)
                            return null;
                        if (a.IsNull)
                            return domain.defaultValue;
                        TypedValue b = op2.Eval(cx)?.NotNull();
                        if (b == null)
                            return null;
                        if (b.IsNull || op1.domain.Compare(a, b) != 0)
                            return a;
                        return domain.defaultValue;
                    }
                case Sqlx.OCTET_LENGTH:
                    {
                        v = val?.Eval(cx)?.NotNull();
                        if (v == null)
                            return null;
                        if (v.Val() is byte[] bytes)
                            return new TInt(bytes.Length);
                        return domain.defaultValue;
                    }
                case Sqlx.OVERLAY:
                    v = val?.Eval(cx)?.NotNull();
                    return v; //TBD
                case Sqlx.PARTITION:
                        return TNull.Value;
                case Sqlx.POSITION:
                    {
                        if (op1 != null && op2 != null)
                        {
                            string t = op1.Eval(cx)?.ToString();
                            string s = op2.Eval(cx)?.ToString();
                            if (t != null && s != null)
                                return new TInt(s.IndexOf(t));
                            return domain.defaultValue;
                        }
                        return TNull.Value;
                    }
                case Sqlx.POWER:
                    {
                        v = val?.Eval(cx)?.NotNull();
                        if (v == null)
                            return null;
                        var w = op1?.Eval(cx)?.NotNull();
                        if (w == null)
                            return null;
                        if (v.IsNull)
                            return domain.defaultValue;
                        return new TReal(Math.Pow(v.ToDouble(), w.ToDouble()));
                    }
                case Sqlx.PROVENANCE:
                    return TNull.Value;
                case Sqlx.RANK:
                    return new TInt(firstTie._pos + 1);
                case Sqlx.ROW_NUMBER: return new TInt(fc.wrb._pos+1);
                case Sqlx.SET:
                    {
                        v = val?.Eval(cx)?.NotNull();
                        if (v == null)
                            return null;
                        if (!(v is TMultiset))
                            throw new DBException("42113").Mix();
                        TMultiset m = (TMultiset)v;
                        return m.Set(defpos);
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
                        v = val?.Eval(cx)?.NotNull();
                        if (v == null)
                            return null;
                        string sv = v.ToString();
                        var w = op1?.Eval(cx)??null;
                        if (sv == null || w == null)
                            return domain.defaultValue;
                        var x = op2?.Eval(cx)??null;
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
                     v = val?.Eval(cx)?.NotNull();
                    return v; // TBD
                case Sqlx.TRIM:
                    {
                        v = val?.Eval(cx)?.NotNull();
                        if (v == null)
                            return null;
                        if (v.IsNull)
                            return domain.defaultValue;
                        string sv = v.ToString();
                        object c = null;
                        if (op1 != null)
                        {
                            string s = op1.Eval(cx).ToString();
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
                        v = val?.Eval(cx)?.NotNull();
                        if (v == null)
                            return null;
                        if (!v.IsNull)
                            return new TChar(v.ToString().ToUpper());
                        return domain.defaultValue;
                    }
            /*    case Sqlx.VERSIONING: // row version pseudocolumn
                    {
                        var rv = cx.Ctx(cx.cur as From)?.row._Rvv();
                        if (rv != null)
                            return new TInt(rv.off);
                        return TNull.Value;
                    } */
                case Sqlx.WHEN: // searched case
                    {
                        v = val?.Eval(cx)?.NotNull();
                        if (v == null)
                            return null;
                        TypedValue a = op1.Eval(cx);
                        if (a == TBool.True)
                            return v;
                        return op2.Eval(cx);
                    }
                case Sqlx.XMLAGG: return new TChar(fc.sb.ToString());
                case Sqlx.XMLCAST: goto case Sqlx.CAST;
                case Sqlx.XMLCOMMENT:
                    {
                        v = val?.Eval(cx)?.NotNull();
                        if (v == null)
                            return null;
                        return new TChar("<!-- " + v.ToString().Replace("--", "- -") + " -->");
                    }
                //	case Sqlx.XMLCONCAT: break; see SqlValueExpr
                case Sqlx.XMLELEMENT:
                    {
                        object a = op2?.Eval(cx)?.NotNull();
                        object x = val?.Eval(cx)?.NotNull();
                        if (a == null || x == null)
                            return null;
                        string n = XmlConvert.EncodeName(op1.Eval(cx).ToString());
                        string r = "<" + n  + " " + ((a == null) ? "" : XmlEnc(a)) + ">" +
                            ((x == null) ? "" : XmlEnc(x)) + "</" + n + ">";
                        //				trans.xmlns = "";
                        return new TChar(r);
                    }
                 case Sqlx.XMLPI:
                    v = val?.Eval(cx)?.NotNull();
                    if (v == null)
                        return null;
                    return new TChar("<?" + v + " " + op1.Eval(cx) + "?>");
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
                        v = val?.Eval(cx)?.NotNull();
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
        internal override void StartCounter(Context _cx,RowSet rs)
        {
            var fd = _cx.func[defpos];
            if (fd == null)
                _cx.func += (defpos, fd = new FunctionData());
            //         var pkey = new PosTRow(rs.defpos, (TRow)_cx.values[rs.defpos]);
            var rw = (rs is EvalRowSet)?new TRow(rs.info):(TRow)_cx.values[rs.defpos];
            if (fd.regs == null)
                fd.regs = new CTree<TRow, FunctionData.Register>(domain.kind);
            var fc = fd.regs[rw]; /*[rs.defpos]?[rw];
            if (fc == null)
            {
                var t = fd.regs[rs.defpos] ?? new CTree<TRow, FunctionData.Register>(Sqlx.INT);
                t += (rw,fc = new FunctionData.Register());
                fd.regs += (rs.defpos, t);
            } */
            if (fc == null)
                fd.regs += (rw, fc = new FunctionData.Register());
            fc.acc1 = 0.0;
            fc.mset = null;
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
                    fc.mset = new TMultiset(val.domain);
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
                    fc.acc = new TArray(new Domain(defpos,Sqlx.ARRAY, Domain.Content));
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
                    fc.sumInteger = null;
                    break;
                default:
                    val?.StartCounter(_cx, rs);
                    break;
            }
        }
        /// <summary>
        /// for aggregates we need to implement AddIn
        /// </summary>
        /// <param name="ws">the window specification</param>
        internal void StartCounter(Context _cx,RowSet rs, WindowSpecification ws)
        {
            switch (kind)
            {
                default: StartCounter(_cx,rs); break;
            }
        }
        internal override void _AddIn(Context cx,Cursor rb,TRow key,ref BTree<long,bool?>aggsDone)
        {
            if (aggsDone[defpos] == true)
                return;
            if (key == null)
            {
                var rs = cx.data[rb._rowsetpos];
                key = new TRow(rs.keyInfo);
            }
            var tr = cx.db as Transaction;
            if (tr == null)
                return;
            var fd = cx.func[defpos];
            if (fd == null)
                cx.func += (defpos, fd = new FunctionData());
            //            var rp = cx.profile[defpos];
            var fc = fd.regs[key]; //[rp]?[key];
//            if (fc==null)
 //           {
//                t += (key, fc = new FunctionData.Register());
//                fd.regs += (rp, t);
//            }
            aggsDone +=(defpos, true);
            if (filter != null && !filter.Matches(cx))
                return;
            if (mod == Sqlx.DISTINCT)
            {
                var v = val.Eval(cx)?.NotNull();
                if (v != null)
                {
                    if (fd.dset == null)
                        fd.dset = new TMultiset(v.dataType);
                    else if (fd.dset.Contains(v))
                        return;
                    fd.dset.Add(v);
         //           etag = ETag.Add(v.etag, etag);
                }
            }
            switch (kind)
            {
                case Sqlx.AVG: // is not used with Remote
                    {
                        var v = val.Eval(cx);
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
                        if (window != null)
                            goto case Sqlx.COLLECT;
                        var v = val.Eval(cx)?.NotNull();
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
                    if (val != null)
                    {
                        if (fc.acc == null)
                            fc.acc = new TArray(new Domain(defpos,Sqlx.ARRAY, val.domain));
                        var ar = fc.acc as TArray;
                        var v = val.Eval(cx)?.NotNull();
                        if (v != null)
                        {
                            ar.list.Add(v);
                //            etag = ETag.Add(v.etag, etag);
                        }
               //         else
                //            (tr as Transaction)?.Warning("01003");
                    }
                    break;
                case Sqlx.COLLECT:
                    {
                        if (val != null)
                        {
                            if (fc.mset == null && val.Eval(cx) != null)
                                fc.mset = new TMultiset(val.domain);
                            var v = val.Eval(cx)?.NotNull();
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
                        var v = val.Eval(cx)?.NotNull();
                        if (v != null && !v.IsNull)
                        {
                            fc.count++;
            //                etag = ETag.Add(v.etag, etag);
                        }
                    }
                    break;
                case Sqlx.EVERY:
                    {
                        var v = val.Eval(cx)?.NotNull();
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
                    if (val != null && fc.acc == null)
                    {
                        fc.acc = val.Eval(cx)?.NotNull();
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
                            var vv = val.Eval(cx)?.NotNull();
                            if (vv == null || vv.IsNull)
                                fc.mset = new TMultiset(val.domain.elType.domain); // check??
            //                else
            //                    (tr as Transaction)?.Warning("01003");
                        }
                        else
                        {
                            var v = val.Eval(cx)?.NotNull();
                            fc.mset = TMultiset.Union(fc.mset, v as TMultiset, true);
              //              etag = ETag.Add(v?.etag, etag);
                        }
                        break;
                    }
                case Sqlx.INTERSECTION:
                    {
                        var v = val.Eval(cx)?.NotNull();
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
                    if (val != null)
                    {
                        fc.acc = val.Eval(cx)?.NotNull();
                 //       if (fd.cur.acc != null)
                 //       {
                //            domain = cur.acc.dataType;
                //            etag = ETag.Add(val.etag, etag);
                 //       }
                    }
                    break;
                case Sqlx.MAX:
                    {
                        TypedValue v = val.Eval(cx)?.NotNull();
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
                        TypedValue v = val.Eval(cx)?.NotNull();
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
                        var o = val.Eval(cx);
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
                        var v = val.Eval(cx)?.NotNull();
                        if (v==null)
                        {
                //            tr.Warning("01003");
                            return;
                        }
               //         etag = ETag.Add(v.etag, etag);
                        switch (fc.sumType.kind)
                        {
                            case Sqlx.CONTENT:
                                if (v is TInt)
                                {
                                    fc.sumType = Domain.Int;
                                    fc.sumLong = v.ToLong().Value;
                                    return;
                                }
                                if (v is TInteger)
                                {
                                    fc.sumType = Domain.Int;
                                    fc.sumInteger = (Integer)v.Val();
                                    return;
                                }
                                if (v is TReal)
                                {
                                    fc.sumType = Domain.Real;
                                    fc.sum1 = ((TReal)v).dvalue;
                                    return;
                                }
                                if (v is TNumeric)
                                {
                                    fc.sumType = Domain.Numeric;
                                    fc.sumDecimal = ((TNumeric)v).value;
                                    return;
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
                                    return;
                                }
                                if (v is TInteger)
                                {
                                    Integer a = ((TInteger)v).ivalue;
                                    if (fc.sumInteger == null)
                                        fc.sumInteger = new Integer(fc.sumLong) + a;
                                    else
                                        fc.sumInteger = fc.sumInteger + a;
                                    return;
                                }
                                break;
                            case Sqlx.REAL:
                                if (v is TReal)
                                {
                                    fc.sum1 += ((TReal)v).dvalue;
                                    return;
                                }
                                break;
                            case Sqlx.NUMERIC:
                                if (v is TNumeric)
                                {
                                    fc.sumDecimal = fc.sumDecimal + ((TNumeric)v).value;
                                    return;
                                }
                                break;
                        }
                        throw new DBException("22108").Mix();
                    }
                case Sqlx.XMLAGG:
                    {
                        fc.sb.Append(' ');
                        var o = val.Eval(cx)?.NotNull();
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
                    val?.AddIn(cx,rb,null);
                    break;
            }
        }
        /// <summary>
        /// Window Functions: bmk is a bookmark in cur.wrs
        /// </summary>
        /// <param name="bmk"></param>
        /// <returns></returns>
        bool InWindow(Context _cx, RTreeBookmark bmk, FunctionData.Register fc)
        {
            if (bmk == null)
                return false;
            var tr = _cx.db;
            if (window.units == Sqlx.RANGE && !(TestStartRange(_cx,bmk,fc) && TestEndRange(_cx,bmk,fc)))
                return false;
            if (window.units == Sqlx.ROWS && !(TestStartRows(_cx,bmk,fc) && TestEndRows(_cx,bmk,fc)))
                return false;
            return true;
        }
        /// <summary>
        /// Test the window against the end of the given rows measured from cur.wrb
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestEndRows(Context _cx, RTreeBookmark bmk,FunctionData.Register fc)
        {
            if (window.high == null || window.high.unbounded)
                return true;
            long limit;
            if (window.high.current)
                limit = fc.wrb._pos;
            else if (window.high.preceding)
                limit = fc.wrb._pos - (window.high.distance?.ToLong()??0);
            else
                limit = fc.wrb._pos + (window.high.distance?.ToLong()??0);
            return bmk._pos <= limit; 
        }
        /// <summary>
        /// Test a window against the start of a rows
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestStartRows(Context _cx, RTreeBookmark bmk,FunctionData.Register fc)
        {
            if (window.low == null || window.low.unbounded)
                return true;
            long limit;
            if (window.low.current)
                limit =fc.wrb._pos;
            else if (window.low.preceding)
                limit = fc.wrb._pos - (window.low.distance?.ToLong() ?? 0);
            else
                limit = fc.wrb._pos + (window.low.distance?.ToLong() ?? 0);
            return bmk._pos >= limit;
        }

        /// <summary>
        /// Test the window against the end of the given range
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestEndRange(Context _cx, RTreeBookmark bmk, FunctionData.Register fc)
        {
            if (window.high == null || window.high.unbounded)
                return true;
            var n = val.defpos;
            var kt = val.domain;
            var wrv = fc.wrb[n];
            TypedValue limit;
            var tr = _cx.db as Transaction;
            if (tr == null)
                return false;
            if (window.high.current)
                limit = wrv;
            else if (window.high.preceding)
                limit = kt.Eval(defpos,wrv, (kt.AscDesc == Sqlx.ASC) ? Sqlx.MINUS : Sqlx.PLUS, window.high.distance);
            else
                limit = kt.Eval(defpos,wrv, (kt.AscDesc == Sqlx.ASC) ? Sqlx.PLUS : Sqlx.MINUS, window.high.distance);
            return kt.Compare(bmk[n], limit) <= 0; 
        }
        /// <summary>
        /// Test a window against the start of a range
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestStartRange(Context _cx, RTreeBookmark bmk,FunctionData.Register fc)
        {
            if (window.low == null || window.low.unbounded)
                return true;
            var n = val.defpos;
            var kt = val.domain;
            var tv = fc.wrb[n];
            TypedValue limit;
            var tr = _cx.db as Transaction;
            if (tr == null)
                return false;
            if (window.low.current)
                limit = tv;
            else if (window.low.preceding)
                limit = kt.Eval(defpos,tv, (kt.AscDesc != Sqlx.DESC) ? Sqlx.PLUS : Sqlx.MINUS, window.low.distance);
            else
                limit = kt.Eval(defpos,tv, (kt.AscDesc != Sqlx.DESC) ? Sqlx.MINUS : Sqlx.PLUS, window.low.distance);
            return kt.Compare(bmk[n], limit) >= 0; // OrderedKey comparison
        }
        /// <summary>
        /// Alas, we can't use Value() here, as the value is still in progress.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns>whether b ties a</returns>
        bool Ties(Cursor a,RTreeBookmark b)
        {
            for (var i = window.partition; i < window.order.items.Length; i++)
            {
                var oi = window.order.items[i];
                var n = oi.defpos;
                var av = a[n];
                var bv = b[n];
                if (av==bv)
                    continue;
                if (av == null)
                    return false;
                if (av.CompareTo(bv) != 0)
                    return false;
            }
            return true;
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
        internal override Selection Needs(Selection qn)
        {
            qn = val?.Needs(qn) ?? qn;
            qn = op1?.Needs(qn) ?? qn;
            qn = op2?.Needs(qn) ?? qn;
            return qn;
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
            var sb = new StringBuilder(Flag());
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
        /// <summary>
        /// See notes on SqlHttpUsing class: we are generating columns for the contrib query.
        /// </summary>
        /// <param name="gf">The query: From with a RestView target</param>
        /// <returns></returns>
        internal override SqlValue _ColsForRestView(long dp, Context cx,From gf, GroupSpecification gs, 
            ref BTree<SqlValue, string> gfc, ref BTree<SqlValue, string> rem, 
            ref BTree<string, bool?> reg,ref BTree<long,SqlValue>map)
        {
            var ac = "C_" + defpos;
            var qs = (Query)cx.obs[gf.QuerySpec(cx)];
            var cs = gf.source as CursorSpecification;
            var an = alias ?? ac;
            switch (kind)
            {
                case Sqlx.AVG:
                    {
                        var n0 = ac;
                        var n1 = "D_" + defpos;
                        var c0 = new SqlFunction(dp, Sqlx.SUM, val, null, null, Sqlx.NO,
                            new BTree<long,object>(_Alias,n0));
                        var c1 = new SqlFunction(dp, Sqlx.COUNT, val, null, null, Sqlx.NO,
                            new BTree<long, object>(_Alias, n1));
                        rem = rem+(c0, n0)+(c1, n1);
                        var s0 = new SqlValue(dp, n0, domain, BTree<long,object>.Empty
                            + (Query,gf));
                        var s1 = new SqlValue(dp, n1, Domain.Int,BTree<long, object>.Empty
                            + (Query, gf));
                        var ct = new SqlValueExpr(dp, Sqlx.DIVIDE,
                                new SqlValueExpr(dp, Sqlx.TIMES,
                                    new SqlFunction(dp, Sqlx.SUM, s0, null, null, Sqlx.NO),
                                    new SqlLiteral(dp, new TReal(Domain.Real, 1.0)), Sqlx.NO),
                                new SqlFunction(dp, Sqlx.SUM, s1, null, null, Sqlx.NO), Sqlx.NO,
                            new BTree<long, object>(_Alias, an));
                        gfc=gfc+(s0, n0)+(s1, n1);
                        map+=(defpos, ct);
                        return ct;
                    }
                case Sqlx.EXTRACT:
                    {
                        var ct = new SqlFunction(dp, mod, val, null, null, Sqlx.NO, 
                        new BTree<long,object>(_Alias,an));
                        SqlValue st = ct;
                        rem+=(ct, an);
                        st = new SqlValue(dp, an, Domain.Int, BTree<long, object>.Empty
                            + (Query, gf));
                        gfc+=(st, an);
                        map+=(defpos, st);
                        return st;
                    }
                case Sqlx.STDDEV_POP:
                    {
                        var n0 = ac;
                        var n1 = "D_" + defpos;
                        var n2 = "E_" + defpos;
                        var c0 = new SqlFunction(dp, Sqlx.SUM, val, null, null, Sqlx.NO,
                            new BTree<long,object>(_Alias,n0));
                        var c1 = new SqlFunction(dp, Sqlx.COUNT, val, null, null, Sqlx.NO,
                            new BTree<long,object>(_Alias,n1));
                        var c2 = new SqlFunction(dp, Sqlx.SUM,
                            new SqlValueExpr(dp, Sqlx.TIMES, val, val, Sqlx.NO), null, null, Sqlx.NO,
                        new BTree<long, object>(_Alias,n2));
                        rem = rem+(c0, n0)+(c1, n1)+(c2, n2);
                        // c0 is SUM(x), c1 is COUNT, c2 is SUM(X*X)
                        // SQRT((c2-2*c0*xbar+xbar*xbar)/c1)
                        var s0 = new SqlValue(dp, n0, domain, BTree<long, object>.Empty
                            + (Query, gf));
                        var s1 = new SqlValue(dp, n1, domain, BTree<long, object>.Empty
                            + (Query, gf));
                        var s2 = new SqlValue(dp, n2, domain, BTree<long, object>.Empty
                            + (Query, gf));
                        var xbar = new SqlValueExpr(dp, Sqlx.DIVIDE, s0, s1, Sqlx.NO);
                        var cu = new SqlFunction(dp, Sqlx.SQRT,
                            new SqlValueExpr(dp, Sqlx.DIVIDE,
                                new SqlValueExpr(dp, Sqlx.PLUS,
                                    new SqlValueExpr(dp, Sqlx.MINUS, s2,
                                        new SqlValueExpr(dp,Sqlx.TIMES,xbar,
                                            new SqlValueExpr(dp, Sqlx.TIMES, s0, 
                                                new SqlLiteral(dp, new TReal(Domain.Real, 2.0)), 
                                                Sqlx.NO),
                                            Sqlx.NO),
                                        Sqlx.NO),
                                    new SqlValueExpr(dp, Sqlx.TIMES, xbar, xbar, Sqlx.NO),
                                    Sqlx.NO),
                                s1, Sqlx.NO), 
                            null,null,Sqlx.NO);
                        gfc = gfc +(s0, n0)+(s1, n1)+(s2, n2);
                        map+=(defpos, cu);
                        return cu;
                    }
                default:
                    {
                        if (aggregates0())
                        {
                            var nk = kind;
                            var vt = val?.domain;
                            var vn = ac;
                            if (kind == Sqlx.COUNT)
                            {
                                nk = Sqlx.SUM;
                                vt = Domain.Int;
                            }
                            var st = this + (_Alias,ac);
                            rem+=(st, ac);
                            var va = new SqlValue(dp, vn, vt, BTree<long,object>.Empty
                                +(Query, gf));
                            var sf = new SqlFunction(dp, nk, 
                                va, op1, op2, mod,new BTree<long,object>(_Alias,an));
                            gfc+=(va, vn);
                            map+=(defpos, sf);
                            return sf;
                        }
                        if (aggregates())
                        {
                            var sr = new SqlFunction(dp, kind,
                                val._ColsForRestView(dp, cx, gf, gs, ref gfc, ref rem, ref reg, ref map),
                                op1, op2, mod, new BTree<long, object>(_Alias, an));
                            map+=(defpos, sr);
                            return sr;
                        }
                        var r = this+(_Alias,an);
                        gfc+=(r, an);
                        rem+=(r, an);
                        var sn = new SqlValue(dp,an,domain, BTree<long,object>.Empty
                            +(Query,gf));
                        map+=(defpos, sn);
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
        internal override bool aggregates()
        {
            return aggregates0() || (val?.aggregates()==true) || (op1?.aggregates()==true) || (op2?.aggregates()==true);
        }
    }
    /// <summary>
    /// The Parser converts this n-adic function to a binary one
    /// </summary>
    internal class SqlCoalesce : SqlFunction
    {
        internal SqlCoalesce(long dp, SqlValue op1, SqlValue op2)
            : base(dp, Sqlx.COALESCE, null, op1, op2, Sqlx.NO) { }
        protected SqlCoalesce(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCoalesce operator+(SqlCoalesce s,(long,object)x)
        {
            return new SqlCoalesce(s.defpos, s.mem + x);
        }
        internal override TypedValue Eval(Context cx)
        {
            return (op1.Eval(cx) is TypedValue lf) ? 
                ((lf == TNull.Value) ? op2.Eval(cx) : lf) : null;
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
        internal SqlTypeUri(long dp, SqlValue op1)
            : base(dp, Sqlx.TYPE_URI, null, op1, null, Sqlx.NO) { }
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
            if (op1 != null)
                v = op1.Eval(cx);
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
            High = -351, // SqlValue
            Low = -352, // SqlValue
            Op = -353, // Sqlx
            Select = -354, //QuerySpecification
            Vals = -355, //BList<SqlValue>
            What = -356, //SqlValue
            Where = -357; // SqlValue
        public SqlValue what => (SqlValue)mem[What];
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
        public QuerySpecification select => (QuerySpecification)mem[Select];
        /// <summary>
        /// A new Quantified Predicate built by the parser (or by Copy, Invert here)
        /// </summary>
        /// <param name="w">The test expression</param>
        /// <param name="sv">the comparison operator, or AT</param>
        /// <param name="a">whether ALL has been specified</param>
        /// <param name="s">the query specification to test against</param>
        internal QuantifiedPredicate(long defpos,SqlValue w, Sqlx o, bool a, QuerySpecification s)
            : base(defpos,BTree<long,object>.Empty+(_Domain,Domain.Bool)
            + (What,w)+(Op,o)+(All,a)+(Select,s)
                  +(Dependents,new BTree<long,bool>(w.defpos,true)+(s.defpos,true))
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
        internal override SqlValue AddReg(Context cx, long rs, TRow key)
        {
            select.AddReg(cx,rs,key);
            return (what.AddReg(cx,rs,key) != null) ? this : null;
        }
        internal override DBObject Relocate(long dp)
        {
            return new QuantifiedPredicate(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = (QuantifiedPredicate)base.Relocate(wr);
            var w = (SqlValue)what.Relocate(wr);
            if (w != what)
                r += (What, w);
            var s = (QuerySpecification)select.Relocate(wr);
            if (s != select)
                r += (Select, s);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (QuantifiedPredicate)base._Replace(cx, so, sv);
            var wh = (SqlValue)r.what._Replace(cx,so,sv);
            if (wh != r.what)
                r += (What, wh);
            var se = r.select._Replace(cx, so, sv);
            if (se != r.select)
                r += (Select, se);
            cx.done += (defpos, r);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (QuantifiedPredicate)base.AddFrom(cx, q);
            var a = r.what.AddFrom(cx, q);
            if (a != r.what)
                r += (What, a);
            return (SqlValue)cx.Add(r);
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (QuantifiedPredicate)base.Frame(cx);
            var wh = (SqlValue)r.what.Frame(cx);
            if (wh != r.what)
                r += (What, wh);
            var se = r.select.Frame(cx);
            if (se != r.select)
                r += (Select, se);
            return cx.Add(r,true);
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
                case Sqlx.EQL: return new QuantifiedPredicate(defpos, what, Sqlx.NEQ, !all, select);
                case Sqlx.NEQ: return new QuantifiedPredicate(defpos, what, Sqlx.EQL, !all, select);
                case Sqlx.LEQ: return new QuantifiedPredicate(defpos, what, Sqlx.GTR, !all, select);
                case Sqlx.LSS: return new QuantifiedPredicate(defpos, what, Sqlx.GEQ, !all, select);
                case Sqlx.GEQ: return new QuantifiedPredicate(defpos, what, Sqlx.LSS, !all, select);
                case Sqlx.GTR: return new QuantifiedPredicate(defpos, what, Sqlx.LEQ, !all, select);
                default: throw new PEException("PE65");
            }
        }
        /// <summary>
        /// Analysis stage Conditions: process conditions
        /// </summary>
        internal override Query Conditions(Context cx, Query q,bool disj,out bool move)
        {
            move = false;
            return what.Conditions(cx, q, false, out _);
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            for (var rb = select.RowSets(cx).First(cx); rb != null; rb = rb.Next(cx))
            {
                var col = rb[0];
                if (what.Eval(cx) is TypedValue w)
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
        internal override bool aggregates()
        {
            return what.aggregates()||select.aggregates();
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            what.Build(_cx,rs);
            select.Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx, RowSet rs)
        {
            what.StartCounter(_cx, rs);
            select.StartCounter(_cx, rs);
        }
        internal override void _AddIn(Context _cx,Cursor rb, TRow key,
            ref BTree<long, bool?> aggsDone)
        {
            what._AddIn(_cx,rb, key, ref aggsDone);
            select.AddIn(_cx,rb,key);
        }
        /// <summary>
        /// We aren't a column reference. If the select needs something
        /// From will add it to cx.needed
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            qn = what.Needs(qn);
            return qn;
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
        public SqlValue what =>(SqlValue)mem[QuantifiedPredicate.What];
        /// <summary>
        /// BETWEEN or NOT BETWEEN
        /// </summary>
        public bool between => (bool)(mem[QuantifiedPredicate.Between]??false);
        /// <summary>
        /// The low end of the range of values specified
        /// </summary>
        public SqlValue low => (SqlValue)mem[QuantifiedPredicate.Low];
        /// <summary>
        /// The high end of the range of values specified
        /// </summary>
        public SqlValue high => (SqlValue)mem[QuantifiedPredicate.High];
        /// <summary>
        /// A new BetweenPredicate from the parser
        /// </summary>
        /// <param name="w">the test expression</param>
        /// <param name="b">between or not between</param>
        /// <param name="a">The low end of the range</param>
        /// <param name="sv">the high end of the range</param>
        internal BetweenPredicate(long defpos,SqlValue w, bool b, SqlValue a, SqlValue h)
            : base(defpos,BTree<long,object>.Empty+(_Domain,Domain.Bool)
                  +(QuantifiedPredicate.What,w)+(QuantifiedPredicate.Between,b)
                  +(QuantifiedPredicate.Low,a)+(QuantifiedPredicate.High,h)
                  +(Dependents,new BTree<long,bool>(w.defpos,true)+(a.defpos,true)+(h.defpos,true))
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
        internal override Basis Relocate(Writer wr)
        {
            var r = (BetweenPredicate)base.Relocate(wr);
            var w = (SqlValue)what?.Relocate(wr);
            if (w != what)
                r += (QuantifiedPredicate.What, w);
            var lw = (SqlValue)low?.Relocate(wr);
            if (lw!=low)
                r += (QuantifiedPredicate.Low, lw);
            var hi = (SqlValue)high?.Relocate(wr);
            if (hi != high)
                r += (QuantifiedPredicate.High, hi);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (BetweenPredicate)base._Replace(cx, so, sv);
            var wh = (SqlValue)r.what._Replace(cx,so,sv);
            if (wh != r.what)
                r += (QuantifiedPredicate.What, wh);
            var lw = (SqlValue)r.low._Replace(cx,so,sv);
            if (lw != r.low)
                r += (QuantifiedPredicate.Low, lw);
            var hg = (SqlValue)r.high._Replace(cx,so,sv);
            if (hg != r.high)
                r += (QuantifiedPredicate.High, hg);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (BetweenPredicate)base.Frame(cx);
            var wh = (SqlValue)r.what.Frame(cx);
            if (wh != r.what)
                r += (QuantifiedPredicate.What, wh);
            var lw = (SqlValue)r.low.Frame(cx);
            if (lw != r.low)
                r += (QuantifiedPredicate.Low, lw);
            var hg = (SqlValue)r.high.Frame(cx);
            if (hg != r.high)
                r += (QuantifiedPredicate.High, hg);
            cx.done += (defpos, r);
            return cx.Add(r,true);
        }
        internal override bool Uses(long t)
        {
            return low.Uses(t) || high.Uses(t) || what.Uses(t);
        }
        internal override SqlValue AddReg(Context cx, long rs, TRow key)
        {
            var a = what.AddReg(cx,rs,key) != null;
            var b = low.AddReg(cx,rs,key) != null;
            var c = high.AddReg(cx,rs,key) != null;
            return (a || b || c) ? this : null; // avoid shortcut evaluation
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from < 0)
                return this;
            var r = (BetweenPredicate)base.AddFrom(cx, q);
            var a = r.what.AddFrom(cx, q);
            if (a != r.what)
                r += (QuantifiedPredicate.What, a);
            a = r.low.AddFrom(cx, q);
            if (a != r.low)
                r += (QuantifiedPredicate.Low, a);
            a = r.high.AddFrom(cx, q);
            if (a != r.high)
                r += (QuantifiedPredicate.High, a);
            return (SqlValue)cx.Add(r);
        }
        /// <summary>
        /// Invert the between predicate (for part condition)
        /// </summary>
        /// <returns>the new search</returns>
        internal override SqlValue Invert()
        {
            return new BetweenPredicate(defpos,what, !between, low, high);
        }
        internal override bool aggregates()
        {
            return what.aggregates()||low.aggregates()||high.aggregates();
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            what.Build(_cx,rs);
            low.Build(_cx,rs);
            high.Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx,RowSet rs)
        {
            what.StartCounter(_cx,rs);
            low.StartCounter(_cx,rs);
            high.StartCounter(_cx,rs);
        }
        internal override void _AddIn(Context _cx,Cursor rb, TRow key, 
            ref BTree<long, bool?> aggsDone)
        {
            what._AddIn(_cx,rb, key, ref aggsDone);
            low._AddIn(_cx,rb, key, ref aggsDone);
            high._AddIn(_cx,rb, key, ref aggsDone);
        }
        internal override void OnRow(Cursor bmk)
        {
            what.OnRow(bmk);
            low.OnRow(bmk);
            high.OnRow(bmk);
        }
        /// <summary>
        /// Analysis stage Conditions: support distribution of conditions to froms etc
        /// </summary>
        internal override Query Conditions(Context cx,Query q,bool disj,out bool move)
        {
            move = false;
            q = what.Conditions(cx, q, false,out _);
            if (low!=null)
                q = low.Conditions(cx, q, false, out _);
            if (high!=null)
                q = high.Conditions(cx, q, false, out _);
            return q;
        }
         /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (what.Eval(cx) is TypedValue w)
            {
                var t = what.domain;
                if (low.Eval(cx) is TypedValue lw)
                {
                    if (t.Compare(w, t.Coerce(lw)) < 0)
                        return TBool.False;
                    if (high.Eval(cx) is TypedValue hg)
                        return TBool.For(t.Compare(w, t.Coerce(hg)) <= 0);
                }
            }
            return null;
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            qn = what.Needs(qn);
            qn = low?.Needs(qn)??qn;
            qn = high?.Needs(qn)??qn;
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
    /// </summary>
    internal class LikePredicate : SqlValue
    {
        internal const long
            Escape = -358, // SqlValue
            _Like = -359; // bool
        /// <summary>
        /// like or not like
        /// </summary>
        public bool like => (bool)(mem[_Like]??false);
        /// <summary>
        /// The escape character
        /// </summary>
        public SqlValue escape => (SqlValue)mem[Escape];
        /// <summary>
        /// A like predicate from the parser
        /// </summary>
        /// <param name="a">the left operand</param>
        /// <param name="k">like or not like</param>
        /// <param name="b">the right operand</param>
        /// <param name="e">the escape character</param>
        internal LikePredicate(long dp,SqlValue a, bool k, SqlValue b, SqlValue e)
            : base(dp, new BTree<long,object>(_Domain,Domain.Bool)
                  +(Left,a)+(_Like,k)+(Right,b)+(Escape,e)
                  +(Dependents,new BTree<long,bool>(a.defpos,true)+(b.defpos,true)+(e.defpos,true))
                  +(Depth,1+_Max(a.depth,b.depth,e.depth)))
        { }
        protected LikePredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static LikePredicate operator+(LikePredicate s,(long,object)x)
        {
            return new LikePredicate(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new LikePredicate(defpos,m);
        }
        internal override SqlValue AddReg(Context cx, long rs, TRow key)
        {
            return (left.AddReg(cx,rs,key) != null 
                || right.AddReg(cx,rs,key) != null) ? this : null;
        }
        internal override DBObject Relocate(long dp)
        {
            return new LikePredicate(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var e = (SqlValue)escape.Relocate(wr);
            if (e != escape)
                r += (Escape, e);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (LikePredicate)base._Replace(cx, so, sv);
            var wh = r.left._Replace(cx,so,sv);
            if (wh != r.left)
                r += (Left, wh);
            var rg = r.right._Replace(cx, so, sv);
            if (rg != r.right)
                r += (Right, rg);
            var esc = r.escape._Replace(cx, so, sv);
            if (esc != r.escape)
                r += (Escape, esc);
            cx.done += (defpos, r);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from>0)
                return this;
            var r = (LikePredicate)base.AddFrom(cx, q);
            var a = r.escape.AddFrom(cx, q);
            if (a != r.escape)
                r += (Escape, a);
            a = r.left.AddFrom(cx, q);
            if (a != r.left)
                r += (Left, a);
            a = r.right.AddFrom(cx, q);
            if (a != r.right)
                r += (Right, a);
            return (SqlValue)cx.Add(r);
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (LikePredicate)base.Frame(cx);
            var wh = r.left.Frame(cx);
            if (wh != r.left)
                r += (Left, wh);
            var rg = r.right.Frame(cx);
            if (rg != r.right)
                r += (Right, rg);
            var esc = r.escape.Frame(cx);
            if (esc != r.escape)
                r += (Escape, esc);
            return cx.Add(r,true);
        }
        internal override bool Uses(long t)
        {
            return left.Uses(t) || right.Uses(t) || (escape?.Uses(t)==true);
        }
        /// <summary>
        /// Invert the search (for the part condition)
        /// </summary>
        /// <returns>the new search</returns>
        internal override SqlValue Invert()
        {
            return new LikePredicate(defpos,left, !like, right, escape);
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
            if (left.Eval(cx)?.NotNull() is TypedValue lf && right.Eval(cx)?.NotNull() is TypedValue rg)
            {
                if (lf.IsNull && rg.IsNull)
                    r = true;
                else if ((!lf.IsNull) & !rg.IsNull)
                {
                    string a = lf.ToString();
                    string b = rg.ToString();
                    string e = "\\";
                    if (escape != null)
                        e = escape.Eval(cx).ToString();
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
        internal override bool aggregates()
        {
            return left.aggregates() || right.aggregates();
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            left.Build(_cx,rs);
            right.Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx, RowSet rs)
        {
            left.StartCounter(_cx, rs);
            right.StartCounter(_cx, rs);
        }
        internal override void _AddIn(Context _cx,Cursor rb, TRow key, 
            ref BTree<long, bool?> aggsDone)
        {
            left._AddIn(_cx,rb, key, ref aggsDone);
            right._AddIn(_cx,rb, key, ref aggsDone);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            qn = left.Needs(qn);
            qn = right.Needs(qn);
            qn = escape?.Needs(qn) ?? qn;
            return qn;
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(left);
            if (!like)
                sb.Append(" not");
            sb.Append(" like ");
            sb.Append(right);
            if (escape!=null)
            {
                sb.Append(" escape "); sb.Append(escape);
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// The InPredicate subclass of SqlValue
    /// </summary>
    internal class InPredicate : SqlValue
    {
        public SqlValue what => (SqlValue)mem[QuantifiedPredicate.What];
        /// <summary>
        /// In or not in
        /// </summary>
        public bool found => (bool)(mem[QuantifiedPredicate.Found]??false);
        /// <summary>
        /// A query should be specified (unless a list of values is supplied instead)
        /// </summary>
        public QuerySpecification where => (QuerySpecification)mem[QuantifiedPredicate.Select]; // or
        /// <summary>
        /// A list of values to check (unless a query is supplied instead)
        /// </summary>
        public BList<SqlValue> vals => (BList<SqlValue>)mem[QuantifiedPredicate.Vals]??BList<SqlValue>.Empty;
        public InPredicate(long dp, SqlValue w, BList<SqlValue> vs = null) 
            : base(dp, new BTree<long, object>(_Domain, Domain.Bool)
                  +(QuantifiedPredicate.What,w)+(QuantifiedPredicate.Vals,vs)
                  +(Dependents,_Deps(vs)+(w.defpos,true))+(Depth,1+_Max(w.depth,_Depth(vs))))
        {}
        protected InPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
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
        internal override SqlValue AddReg(Context cx, long rs, TRow key)
        {
            bool nulls = true;
            if (vals != null)
                for (var b =vals.First();b!=null;b=b.Next())
                    if (b.value().AddReg(cx,rs,key) != null)
                        nulls = false;
            return (what.AddReg(cx,rs, key) == null && nulls) ? null : this;
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = (InPredicate)base.Relocate(wr);
            var w = (SqlValue)what.Relocate(wr);
            if (w != what)
                r += (QuantifiedPredicate.What, w);
            var wh = (QuerySpecification)where.Relocate(wr);
            if (wh != where)
                r += (QuantifiedPredicate.Where, wh);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (InPredicate)base._Replace(cx, so, sv);
            var wh = r.what._Replace(cx,so,sv);
            if (wh != r.what)
                r += (QuantifiedPredicate.What, wh);
            var wr = r.where?._Replace(cx, so, sv);
            if (wr != r.where)
                r += (QuantifiedPredicate.Select, wr);
            var vs = r.vals;
            for (var b=vs.First();b!=null;b=b.Next())
            {
                var v = b.value()._Replace(cx,so,sv);
                if (v != b.value())
                    vs += (b.key(), (SqlValue)v);
            }
            if (vs != r.vals)
                r += (QuantifiedPredicate.Vals, vs);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (InPredicate)base.Frame(cx);
            var wh = r.what.Frame(cx);
            if (wh != r.what)
                r += (QuantifiedPredicate.What, wh);
            var wr = r.where.Frame(cx);
            if (wr != r.where)
                r += (QuantifiedPredicate.Select, wr);
            var vs = r.vals;
            for (var b = vs.First(); b != null; b = b.Next())
            {
                var v = b.value().Frame(cx);
                if (v != b.value())
                    vs += (b.key(), (SqlValue)v);
            }
            if (vs != r.vals)
                r += (QuantifiedPredicate.Vals, vs);
            return cx.Add(r,true);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (InPredicate)base.AddFrom(cx, q);
            var a = r.what.AddFrom(cx, q);
            if (a != r.what)
                r += (QuantifiedPredicate.What, a);
            var vs = r.vals;
            var ch = false;
            for (var b = vs.First(); b != null; b = b.Next())
            {
                var v = b.value().AddFrom(cx,q);
                if (v != b.value())
                    ch = true;
                vs += (b.key(), v);
            }
            if (ch)
                r += (QuantifiedPredicate.Vals, vs);
            return r;
        }
        internal override bool Uses(long t)
        {
            return what.Uses(t) || where.Uses(t);
        }
        /// <summary>
        /// Analysis stage Conditions: check to see what conditions can be distributed
        /// </summary>
        internal override Query Conditions(Context cx, Query q, bool disj, out bool move)
        {
            move = false;
            q = what?.Conditions(cx, q, false, out _);
            for (var v = vals.First(); v != null; v = v.Next())
                q = v.value().Conditions(cx, q, false, out _);
            return q;
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (what.Eval(cx) is TypedValue w)
            {
                if (vals != BList<SqlValue>.Empty)
                {
                    for (var v = vals.First();v!=null;v=v.Next())
                        if (v.value().domain.Compare(w, v.value().Eval(cx)) == 0)
                            return TBool.For(found);
                    return TBool.For(!found);
                }
                else
                {
                    for (var rb = where.RowSets(cx).First(cx); rb != null; rb = rb.Next(cx))
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
        internal override bool aggregates()
        {
            for (var v = vals.First(); v != null; v = v.Next())
                if (v.value().aggregates())
                    return true;
            return what.aggregates() || base.aggregates();
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            for (var v = vals.First(); v != null; v = v.Next())
                v.value().Build(_cx,rs);
            what.Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx,RowSet rs)
        {
            for (var v = vals?.First(); v != null; v = v.Next())
                v.value().StartCounter(_cx,rs);
            what.StartCounter(_cx,rs);
            base.StartCounter(_cx,rs);
        }
        internal override void _AddIn(Context _cx,Cursor rb, TRow key,
            ref BTree<long, bool?> aggsDone)
        {
            for (var v = vals.First(); v != null; v = v.Next())
                v.value()._AddIn(_cx,rb, key, ref aggsDone);
            what._AddIn(_cx,rb, key, ref aggsDone);
            base._AddIn(_cx,rb, key, ref aggsDone);
        }
        /// <summary>
        /// We aren't a column reference. If the where has needed
        /// From will add them to cx.needed
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            qn = what.Needs(qn);
            return qn;
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append(what);
            if (!found)
                sb.Append(" not");
            sb.Append(" in (");
            if (vals != BList<SqlValue>.Empty)
            {
                var cm = "";
                for (var b = vals.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(b.value());
                }
            }
            else
                sb.Append(where);
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
            Lhs = -361, // SqlValue
            Rhs = -362; // SqlValue
        /// <summary>
        /// the test expression
        /// </summary>
        public SqlValue lhs => (SqlValue)mem[Lhs];
        /// <summary>
        /// found or not found
        /// </summary>
        public bool found => (bool)(mem[Found]??false);
        /// <summary>
        /// the right operand
        /// </summary>
        public SqlValue rhs => (SqlValue)mem[Rhs];
        /// <summary>
        /// Constructor: a member predicate from the parser
        /// </summary>
        /// <param name="a">the left operand</param>
        /// <param name="f">found or not found</param>
        /// <param name="b">the right operand</param>
        internal MemberPredicate(long dp,SqlValue a, bool f, SqlValue b)
            : base(dp, new BTree<long,object>(_Domain,Domain.Bool)
                  +(Lhs,a)+(Found,f)+(Rhs,b)+(Depth,1+_Max(a.depth,b.depth))
                  +(Dependents,new BTree<long,bool>(a.defpos,true)+(b.defpos,true)))
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
        internal override SqlValue AddReg(Context cx, long rs, TRow key)
        {
            return (lhs.AddReg(cx,rs,key) != null 
                || rhs.AddReg(cx,rs,key) != null) ? this : null;
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = (MemberPredicate)base.Relocate(wr);
            var lh = (SqlValue)lhs.Relocate(wr);
            if (lh != lhs)
                r += (Lhs, lh);
            var rh = (SqlValue)rhs.Relocate(wr);
            if (rh != rhs)
                r += (Rhs, rh);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (MemberPredicate)base._Replace(cx, so, sv);
            var lf = r.left._Replace(cx,so,sv);
            if (lf != r.left)
                r += (Lhs,lf);
            var rg = r.rhs._Replace(cx,so,sv);
            if (rg != r.rhs)
                r += (Rhs,rg);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (MemberPredicate)base.Frame(cx);
            var lf = r.left.Frame(cx);
            if (lf != r.left)
                r += (Lhs, lf);
            var rg = r.rhs.Frame(cx);
            if (rg != r.rhs)
                r += (Rhs, rg);
            return cx.Add(r,true);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (MemberPredicate)base.AddFrom(cx, q);
            var a = r.lhs.AddFrom(cx, q);
            if (a != r.lhs)
                r += (Lhs, a);
            a = r.rhs.AddFrom(cx, q);
            if (a != r.rhs)
                r += (Rhs, a);
            return (SqlValue)cx.Add(r);
        }
        /// <summary>
        /// Invert the predicate (for joincondition)
        /// </summary>
        /// <returns>the new search</returns>
        internal override SqlValue Invert()
        {
            return new MemberPredicate(defpos,lhs, !found, rhs);
        }
        /// <summary>
        /// Analysis stage Conditions: see what can be distributed
        /// </summary>
        internal override Query Conditions(Context cx, Query q,bool disj,out bool move)
        {
            move = false;
            q = lhs.Conditions(cx, q, false,out _);
            q = rhs.Conditions(cx, q, false, out _);
            return q;
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            if (lhs.Eval(cx) is TypedValue a && rhs.Eval(cx) is TypedValue b)
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
        internal override bool aggregates()
        {
            return lhs.aggregates()||rhs.aggregates();
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            lhs.Build(_cx,rs);
            rhs.Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx, RowSet rs)
        {
            lhs.StartCounter(_cx, rs);
            rhs.StartCounter(_cx, rs);
        }
        internal override void _AddIn(Context _cx,Cursor rb, TRow key, 
            ref BTree<long, bool?> aggsDone)
        {
            lhs._AddIn(_cx,rb, key, ref aggsDone);
            rhs._AddIn(_cx,rb, key, ref aggsDone);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            qn = lhs.Needs(qn);
            qn = rhs.Needs(qn);
            return qn;
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
        public SqlValue lhs => (SqlValue)mem[MemberPredicate.Lhs];
        /// <summary>
        /// OF or NOT OF
        /// </summary>
        public bool found => (bool)(mem[MemberPredicate.Found]??false);
        /// <summary>
        /// the right operand
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
                  +(MemberPredicate.Lhs,a)+(MemberPredicate.Found,f)
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
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var lh = (SqlValue)lhs.Relocate(wr);
            if (lh != lhs)
                r += (MemberPredicate.Lhs, lh);
            var rh = BList<Domain>.Empty;
            var ch = false;
            for (var b=rhs.First();b!=null;b=b.Next())
            {
                var d = (Domain)b.value().Relocate(wr);
                ch = ch || d != b.value();
                rh += d;
            }
            if (ch)
                r += (MemberPredicate.Rhs, rh);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (TypePredicate)base._Replace(cx, so, sv);
            var lh = r.lhs._Replace(cx, so, sv);
            if (lh != r.lhs)
                r += (MemberPredicate.Lhs, lh);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (TypePredicate)base.Frame(cx);
            var lh = r.lhs.Frame(cx);
            if (lh != r.lhs)
                r += (MemberPredicate.Lhs, lh);
            return cx.Add(r,true);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (TypePredicate)base.AddFrom(cx, q);
            var a = r.lhs.AddFrom(cx, q);
            if (a != r.lhs)
                r += (MemberPredicate.Lhs, a);
            return (SqlValue)cx.Add(r);
        }
        /// <summary>
        /// Invert the predicate (for joincondition)
        /// </summary>
        /// <returns>the new search</returns>
        internal override SqlValue Invert()
        {
            return new TypePredicate(defpos,lhs, !found, rhs);
        }
        /// <summary>
        /// Analysis stage Conditions: see what can be distributed
        /// </summary>
        internal override Query Conditions(Context cx, Query q, bool disj, out bool move)
        {
            move = false;
            q = lhs.Conditions(cx, q, false, out _);
            return q;
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            var a = lhs.Eval(cx);
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
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            return lhs.Needs(qn);
        }
    }
    /// <summary>
    /// SQL2011 defined some new predicates for period
    /// </summary>
    internal class PeriodPredicate : SqlValue
    {
        public PeriodPredicate(long dp,SqlValue op1, Sqlx o, SqlValue op2) 
            :base(dp,BTree<long,object>.Empty+(_Domain,Domain.Bool)
                 +(Left,op1)+(Right,op2)+(Kind,op1)
                 +(Dependents,new BTree<long,bool>(op1.defpos,true)+(op2.defpos,true))
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
        internal override SqlValue AddReg(Context cx, long rs, TRow key)
        {
            return (left?.AddReg(cx,rs,key) != null 
                || right?.AddReg(cx,rs,key) != null) ? this : null;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (PeriodPredicate)base._Replace(cx, so, sv);
            var a = r.left._Replace(cx,so,sv);
            if (a != r.left)
                r += (Left,a);
            var b = r.right._Replace(cx,so,sv);
            if (b != r.right)
                r += (Right,b);
            cx.done += (defpos, r);
            return r;
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (PeriodPredicate)base.AddFrom(cx, q);
            var a = r.left.AddFrom(cx, q);
            if (a != r.left)
                r += (Left, a);
            a = r.right.AddFrom(cx, q);
            if (a != r.right)
                r += (Right, a);
            return (SqlValue)cx.Add(r);
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (PeriodPredicate)base.Frame(cx);
            var a = r.left.Frame(cx);
            if (a != r.left)
                r += (Left, a);
            var b = r.right.Frame(cx);
            if (b != r.right)
                r += (Right, b);
            return cx.Add(r,true);
        }
        internal override bool aggregates()
        {
            return (left?.aggregates()??false)||(right?.aggregates()??false);
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            left?.Build(_cx,rs);
            right?.Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx, RowSet rs)
        {
            left?.StartCounter(_cx, rs);
            right?.StartCounter(_cx, rs);
        }
        internal override void _AddIn(Context _cx,Cursor rb, TRow key, 
            ref BTree<long, bool?> aggsDone)
        {
            left?._AddIn(_cx,rb, key, ref aggsDone);
            right?._AddIn(_cx,rb, key, ref aggsDone);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            qn = left.Needs(qn);
            qn = right.Needs(qn);
            return qn;
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
            QExpr = -363; // Query
        public Query expr => (Query)mem[QExpr];
        /// <summary>
        /// the base query
        /// </summary>
        public QueryPredicate(long dp,Query e,BTree<long,object>m=null) 
            : base(dp, (m??BTree<long,object>.Empty)+(_Domain,Domain.Bool)+(QExpr,e)
                  +(Dependents,new BTree<long,bool>(e.defpos,true))+(Depth,1+e.depth))
        {  }
        protected QueryPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static QueryPredicate operator+(QueryPredicate q,(long,object)x)
        {
            return (QueryPredicate)q.New(q.mem + x);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = (QueryPredicate)base.Relocate(wr);
            var q = (Query)expr.Relocate(wr);
            if (q != expr)
                r += (QExpr, q);
            return r;
        }
        internal override DBObject _Replace(Context cx,DBObject so,DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (QueryPredicate)base._Replace(cx,so,sv);
            var q = (Query)expr._Replace(cx,so,sv);
            if (q != expr)
                r += (QExpr, q);
            cx.done += (defpos, r);
            return r;
        }
        internal override SqlValue AddReg(Context cx, long rs, TRow key)
        {
            expr.AddReg(cx,rs,key);
            return base.AddReg(cx,rs,key);
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (QueryPredicate)base.Frame(cx);
            var q = (Query)expr.Frame(cx);
            if (q != expr)
                r += (QExpr, q);
            return cx.Add(r,true);
        }
        /// <summary>
        /// analysis stage Conditions: analyse the expr (up to building its rowset)
        /// </summary>
        internal override Query Conditions(Context cx, Query q, bool disj, out bool move)
        {
            move = false;
            return expr;
        }
        /// <summary>
        /// if groupby is specified we need to check TableColumns are aggregated or grouped
        /// </summary>
        /// <param name="group"></param>
        internal override bool Check(Context cx, GroupSpecification group)
        {
            var cols = expr.rowType;
            for (var i = 0; i < cols.Length; i++)
                if (cols[i].Check(cx,group))
                    return true;
            return base.Check(cx,group);
        }
        internal override bool aggregates()
        {
            return expr.aggregates();
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            expr.Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx, RowSet rs)
        {
            expr.StartCounter(_cx, rs);
            base.StartCounter(_cx, rs);
        }
        internal override void _AddIn(Context _cx,Cursor rb, TRow key, 
            ref BTree<long, bool?> aggsDone)
        {
            expr.AddIn(_cx,rb,key);
            base._AddIn(_cx,rb, key,ref aggsDone);
        }
        /// <summary>
        /// We aren't a column reference. If there are needs from e.q. where conditions
        /// From will add them to cx.needed.
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            return qn;
        }
    }
    /// <summary>
    /// the EXISTS predicate
    /// </summary>
    internal class ExistsPredicate : QueryPredicate
    {
        public ExistsPredicate(long dp,Query e) : base(dp,e,BTree<long,object>.Empty
            +(Dependents,new BTree<long,bool>(e.defpos,true))+(Depth,1+e.depth)) { }
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
            return TBool.For(expr.RowSets(cx).First(cx)!=null);
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
            var ex = r.expr._Replace(cx, so, sv);
            if (ex != r.expr)
                r += (QExpr, ex);
            cx.done += (defpos, r);
            return r;
        }
        /// <summary>
        /// the predicate is true if the rows are distinct 
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            RowSet rs = expr.RowSets(cx);
            RTree a = new RTree(rs.defpos,rs.info,TreeBehaviour.Disallow, TreeBehaviour.Disallow);
            cx._Add(expr);
            for (var rb=rs.First(cx);rb!= null;rb=rb.Next(cx))
                if (RTree.Add(ref a, rb, rb) == TreeBehaviour.Disallow)
                    return TBool.False;
            return TBool.True;
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
            NVal = -365; //SqlValue
        /// <summary>
        /// the value to test
        /// </summary>
        public SqlValue val => (SqlValue)mem[NVal];
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
                  +(NVal,v)+(NIsNull,b)+(Dependents,new BTree<long,bool>(v.defpos,true))
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
            var a = r.val.AddFrom(cx, q);
            if (a != r.val)
                r += (NVal, a);
            return (SqlValue)cx.Add(r);
        }
        internal override SqlValue AddReg(Context cx, long rs, TRow key)
        {
            return (val.AddReg(cx,rs,key) != null) ? this : null;
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var nv = (SqlValue)val.Relocate(wr);
            if (nv != val)
                r += (NVal, nv);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (NullPredicate)base._Replace(cx, so, sv);
            var vl = r.val._Replace(cx,so,sv);
            if (vl != r.val)
                r += (NVal, vl);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (NullPredicate)base.Frame(cx);
            var vl = r.val.Frame(cx);
            if (vl != r.val)
                r += (NVal, vl);
            return cx.Add(r,true);
        }
        /// <summary>
        /// Test to see if the value is null in the current row
        /// </summary>
        internal override TypedValue Eval(Context cx)
        {
            return (val.Eval(cx) is TypedValue tv)? TBool.For(tv.IsNull == isnull) : null;
        }
        internal override bool aggregates()
        {
            return val.aggregates();
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            val.Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx,RowSet rs)
        {
            val.StartCounter(_cx, rs);
        }
        internal override void _AddIn(Context _cx,Cursor rb, TRow key,
            ref BTree<long, bool?> aggsDone)
        {
            val._AddIn(_cx,rb, key, ref aggsDone);
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            return val.Needs(qn);
        }
        public override string ToString()
        {
            return isnull?"is null":"is not null";
        }
    }
    internal abstract class SqlHttpBase : SqlValue
    {
        internal const long
            GlobalFrom = -255, // From
            HttpWhere = -256, // BTree<long,SqlValue>
            HttpMatches = -257, // BTree<SqlValue,TypedValue>
            HttpRows = -258; // RowSet
        internal From globalFrom => (From)mem[GlobalFrom];
        public BTree<long,SqlValue> where => 
            (BTree<long,SqlValue>)mem[HttpWhere]??BTree<long,SqlValue>.Empty;
        public BTree<SqlValue, TypedValue> matches=>
            (BTree<SqlValue,TypedValue>)mem[HttpMatches]??BTree<SqlValue,TypedValue>.Empty;
        protected RowSet rows => (RowSet)mem[HttpRows]??EmptyRowSet.Value;
        protected SqlHttpBase(long dp, Query q,BTree<long,object> m=null) : base(dp, 
            (m??BTree<long,object>.Empty)+(_Domain,q.domain)+(HttpMatches,q.matches)
            +(GlobalFrom,q))
        { }
        protected SqlHttpBase(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlHttpBase operator+(SqlHttpBase s,(long,object)x)
        {
            return (SqlHttpBase)s.New(s.mem + x);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var gf = (From)globalFrom.Relocate(wr);
            if (gf != globalFrom)
                r += (GlobalFrom, gf);
            var hw = BTree<long, SqlValue>.Empty;
            var ch = false;
            for (var b=where.First();b!=null;b=b.Next())
            {
                var k = wr.Fix(b.key());
                var v = (SqlValue)b.value().Relocate(wr);
                ch = ch || (k != b.key() || v != b.value());
                hw += (k, v);
            }
            if (ch)
                r += (HttpWhere,hw);
            var hm = BTree<SqlValue, TypedValue>.Empty;
            ch = false;
            for (var b = matches.First(); b != null; b = b.Next())
            {
                var k = (SqlValue)b.key().Relocate(wr);
                ch = ch || (k != b.key());
                hm += (k, b.value());
            }
            if (ch)
                r += (HttpMatches, hm);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlHttpBase)base._Replace(cx,so,sv);
            var gf = r.globalFrom._Replace(cx, so, sv);
            if (gf != r.globalFrom)
                r += (GlobalFrom, gf);
            var wh = r.where;
            for (var b=wh.First();b!=null;b=b.Next())
            {
                var v = b.value()._Replace(cx,so,sv);
                if (v != b.value())
                    wh += (b.key(), (SqlValue)v);
            }
            if (wh != r.where)
                r += (HttpWhere, wh);
            var ma = r.matches;
            for (var b=ma.First();b!=null;b=b.Next())
            {
                var v = b.key()._Replace(cx, so, sv);
                if (v != b.key())
                    ma += ((SqlValue)v, b.value());
            }
            if (ma != r.matches)
                r += (HttpMatches, ma);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            var r = (SqlHttpBase)base.Frame(cx);
            var gf = r.globalFrom.Frame(cx);
            if (gf != r.globalFrom)
                r += (GlobalFrom, gf);
            var wh = r.where;
            for (var b = wh.First(); b != null; b = b.Next())
            {
                var v = b.value().Frame(cx);
                if (v != b.value())
                    wh += (b.key(), (SqlValue)v);
            }
            if (wh != r.where)
                r += (HttpWhere, wh);
            var ma = r.matches;
            for (var b = ma.First(); b != null; b = b.Next())
            {
                var v = b.key().Frame(cx);
                if (v != b.key())
                    ma += ((SqlValue)v, b.value());
            }
            if (ma != r.matches)
                r += (HttpMatches, ma);
            return cx.Add(r,true);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (SqlHttpBase)base.AddFrom(cx, q);
            var w = BTree<long, SqlValue>.Empty;
            var ch = false;
            for (var b=r.where.First();b!=null;b=b.Next())
            {
                var a = b.value().AddFrom(cx,q);
                if (a != b.value())
                    ch = true;
                w += (b.key(), a);
            }
            if (ch)
                r += (HttpWhere, w);
            ch = false;
            var m = BTree<SqlValue, TypedValue>.Empty;
            for (var b=r.matches.First();b!=null;b=b.Next())
            {
                var a = b.key().AddFrom(cx, q);
                if (a != b.key())
                ch = true;
                m += (a, b.value());
            }
            if (ch)
                r += (HttpMatches, m);
            return (SqlValue)cx.Add(r);
        }
        internal virtual SqlHttpBase AddCondition(Context cx,SqlValue wh)
        {
            return (wh!=null)? this+(HttpWhere,where+(wh.defpos, wh)):this;
        }
        internal abstract TypedValue Eval(Context cx, bool asArray);
        internal override TypedValue Eval(Context cx)
        {
            return Eval(cx,false);
        }
        internal virtual void Delete(Transaction tr,RestView rv, Query f,BTree<string,bool>dr,Adapters eqs)
        {
        }
        internal virtual void Update(Transaction tr,RestView rv, Query f, BTree<string, bool> ds, Adapters eqs, List<RowSet> rs)
        {
        }
        internal virtual void Insert(RestView rv, Query f, string prov, RowSet data, Adapters eqs, List<RowSet> rs)
        {
        }
        /// <summary>
        /// We aren't a column reference
        /// </summary>
        /// <param name="qn"></param>
        /// <returns></returns>
        internal override Selection Needs(Selection qn)
        {
            for (var b = where.First(); b != null; b = b.Next())
                qn = b.value().Needs(qn);
            for (var b = matches.First(); b != null; b = b.Next())
                qn = b.key().Needs(qn);
            return qn;
        }
    }
    internal class SqlHttp : SqlHttpBase
    {
        internal const long
            KeyType = -370, // ObInfo
            Mime = -371, // string
            Pre = -372, // TRow
            RemoteCols = -373, //string
            TargetType = -374, //ObInfo
            Url = -375; //SqlValue
        public SqlValue expr => (SqlValue)mem[Url]; // for the url
        public string mime=>(string)mem[Mime];
        public TRow pre => (TRow)mem[Pre];
        public ObInfo targetType=> (ObInfo)mem[TargetType];
        public ObInfo keyType => (ObInfo)mem[KeyType];
        public string remoteCols => (string)mem[RemoteCols];
        internal SqlHttp(long dp, Query gf, SqlValue v, string m, 
            BTree<long, SqlValue> w, string rCols, TRow ur = null, BTree<SqlValue, TypedValue> mts = null)
            : base(dp,gf,BTree<long,object>.Empty+(HttpWhere,w)+(HttpMatches,mts)
                  +(Url,v)+(Mime,m)+(Pre,ur)+(RemoteCols,rCols))
        { }
        protected SqlHttp(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlHttp operator+(SqlHttp s,(long,object)x)
        {
            return new SqlHttp(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlHttp(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlHttp(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var kt = (ObInfo)keyType.Relocate(wr);
            if (kt != keyType)
                r += (KeyType, kt);
            var tt = (ObInfo)targetType.Relocate(wr);
            if (tt != targetType)
                r += (TargetType, tt);
            var u = (SqlValue)expr.Relocate(wr);
            if (u != expr)
                r += (Url, u);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlHttp)base._Replace(cx,so,sv);
            var u = r.expr._Replace(cx,so,sv);
            if (u != r.expr)
                r += (Url, u);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject Frame(Context cx)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlHttp)base.Frame(cx);
            var u = r.expr.Frame(cx);
            if (u != r.expr)
                r += (Url, u);
            return cx.Add(r,true);
        }
        internal override SqlValue AddFrom(Context cx, Query q)
        {
            if (from > 0)
                return this;
            var r = (SqlHttp)base.AddFrom(cx, q);
            var a = r.expr.AddFrom(cx, q);
            if (a != r.expr)
                r += (Url, a);
            return (SqlValue)cx.Add(r);
        }
        /// <summary>
        /// A lot of the fiddly rowType calculation is repeated from RestView.RowSets()
        /// - beware of these mutual dependencies
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal override TypedValue Eval(Context cx,bool asArray)
        {
            var r = (expr?.Eval(cx) is TypedValue ev)?
                    OnEval(cx,ev):null;
            if ((!asArray) && r is TArray ta)
            {
                var rs = BList<(long,TRow)>.Empty;
                for (var i = 0; i < ta.list.Count; i++)
                    rs += (cx.nextHeap++, (TRow)ta.list[i]);
                return new ExplicitRowSet(defpos,cx, globalFrom.rowType.info, rs);
            }
            if (r != null)
                return r;
            return TNull.Value;
        }
        TypedValue OnEval(Context cx, TypedValue ev)
        {
            string url = ev.ToString();
            var rx = url.LastIndexOf("/");
            var rtype = globalFrom.source.rowType;
            var vw = cx.tr.objects[globalFrom.target] as View;
            string targetName = "";
            if (globalFrom != null)
            {
                targetName = url.Substring(rx + 1);
                url = url.Substring(0, rx);
            }
            if (url != null)
            {
                var rq = GetRequest(cx, url);
                rq.Method = "POST";
                rq.Accept = mime;
                var sql = new StringBuilder("select ");
                sql.Append(remoteCols);
                sql.Append(" from "); sql.Append(targetName);
                var qs = (Query)cx.obs[globalFrom.QuerySpec(cx)];
                var cm = " group by ";
                if ((vw.remoteGroups != null && vw.remoteGroups.sets.Count > 0) || globalFrom.aggregates())
                {
                    var ids = new List<string>();
                    var cs = globalFrom.source as CursorSpecification;
                    for (var rg = cs.restGroups.First();rg!=null;rg=rg.Next())
                    {
                        var n = rg.key();
                        if (!ids.Contains(n))
                        {
                            ids.Add(n);
                            sql.Append(cm); cm = ",";
                            sql.Append(n);
                        }
                    }
                    if (vw.remoteGroups != null)
                        for(var gs = vw.remoteGroups.sets.First();gs!=null;gs=gs.Next())
                            Grouped(cx, gs.value(), sql, ref cm, ids, globalFrom);
                    for (var b = globalFrom.rowType.First(); b != null; b = b.Next())
                        if (!ids.Contains(b.value().name))
                        {
                            ids.Add(b.value().name);
                            sql.Append(cm); cm = ",";
                            sql.Append(b.key());
                        }
                    var keycols = BList<SqlValue>.Empty;
                    foreach (var id in ids)
                        keycols+=globalFrom.ValFor(cx,id);
          //          keyType = new Domain(keycols);
                    if (globalFrom.source.where.Count > 0 || globalFrom.source.matches.Count > 0)
                    {
                        var sw = globalFrom.WhereString(globalFrom.source.where, globalFrom.source.matches, pre);
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
                    var sw = globalFrom.WhereString(globalFrom.source.where, globalFrom.source.matches, pre);
                    if (sw.Length > 0)
                    {
                        sql.Append(" where ");
                        sql.Append(sw);
                    }
                }
                if (PyrrhoStart.HTTPFeedbackMode)
                    Console.WriteLine(url + " " + sql.ToString());
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
                var et = wr.GetResponseHeader("ETag");
                if (et != null)
                {
        //            tr.etags.Add(et);
                    if (PyrrhoStart.DebugMode)
                        Console.WriteLine("Response ETag: " + et);
                }
                var s = wr.GetResponseStream();
                TypedValue r = null;
                if (s != null)
                    r = rtype.Parse(new Scanner(0,new StreamReader(s).ReadToEnd().ToCharArray(),0));
                if (PyrrhoStart.HTTPFeedbackMode)
                {
                    if (r is TArray)
                        Console.WriteLine("--> " + ((TArray)r).list.Count + " rows");
                    else
                        Console.WriteLine("--> " + (r?.ToString() ?? "null"));
                }
                s.Close();
                return r;
            }
            return null;
        }
        void Grouped(Context cx,Grouping gs,StringBuilder sql,ref string cm,List<string> ids,Query gf)
        {
            for (var b = gs.members.First(); b!=null;b=b.Next())
            {
                var g = b.key();
                if (gf.rowType[g] is SqlValue s && !ids.Contains(s.name))
                {
                    ids.Add(s.name);
                    sql.Append(cm); cm = ",";
                    sql.Append(s.name);
                }
            }
            for (var gi = gs.groups.First();gi!=null;gi=gi.Next())
                Grouped(cx,gi.value(), sql, ref cm,ids, gf);
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
        public static HttpWebRequest GetRequest(Context cx,string url)
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
/*        /// <summary>
        /// Execute a Delete operation (for an updatable REST view)
        /// </summary>
        /// <param name="f">The From</param>
        /// <param name="dr">The delete information</param>
        /// <param name="eqs">equality pairings (e.g. join conditions)</param>
        internal override void Delete(Transaction tr,RestView rv, Query f, BTree<string, bool> dr, Adapters eqs)
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
        internal override void Update(Transaction tr, RestView rv, Query f, BTree<string, bool> ds, Adapters eqs, List<RowSet> rs)
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
        internal override void Insert(RestView rv, Query f, string prov, RowSet data, Adapters eqs, List<RowSet> rs)
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
        */
    }
    /// <summary>
    /// To implement RESTViews properly we need to hack the domain of the FROM globalView.
    /// After stage Selects, globalFrom.domain is as declared in the view definition.
    /// So globalRowSet always has the same rowType as globalfrom,
    /// and the same grouping operation takes place on each remote contributor
    /// </summary>
    internal class SqlHttpUsing : SqlHttpBase
    {
        internal const long
            UsingCols = -259, // BTree<string,long>
            UsingTablePos = -260; // long
        internal long usingtablepos => (long)(mem[usingtablepos] ?? 0);
        internal BTree<string, long> usC =>
            (BTree<string,long>)mem[UsingCols]??BTree<string, long>.Empty;
        // the globalRowSetType is our domain
        /// <summary>
        /// Get our bearings in the RestView (repeating some query analysis)
        /// </summary>
        /// <param name="f"></param>
        /// <param name="ut"></param>
        internal SqlHttpUsing(long dp,Query f,Table ut) 
            : base(dp,f,BTree<long,object>.Empty+(UsingTablePos,ut.defpos))
        { }
        protected SqlHttpUsing(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlHttpUsing operator+(SqlHttpUsing s,(long,object) x)
        {
            return new SqlHttpUsing(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlHttpUsing(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlHttpUsing(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = (SqlHttpUsing)base.Relocate(wr);
            var uc = BTree<string, long>.Empty;
            var ch = false;
            for (var b=usC.First();b!=null;b=b.Next())
            {
                var u = wr.Fix(b.value());
                ch = ch || u != b.value();
                uc += (b.key(), u);
            }
            if (ch)
                r += (UsingCols, uc);
            var ut = wr.Fix(usingtablepos);
            if (ut != usingtablepos)
                r += (UsingTablePos, ut);
            return r;
        }
        internal override DBObject _Replace(Context cx,DBObject so,DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlHttpUsing)base._Replace(cx,so,sv);
            var uc = BTree<string, long>.Empty;
            var ch = false;
            for (var b = usC.First(); b != null; b = b.Next())
            {
                var u = cx.Fix(b.value());
                ch = ch || u != b.value();
                uc += (b.key(), u);
            }
            if (ch)
                r += (UsingCols, uc);
            var ut = cx.Fix(usingtablepos);
            if (ut != usingtablepos)
                r += (UsingTablePos, ut);
            cx.done += (defpos, r);
            return r;
        }
        internal override SqlHttpBase AddCondition(Context cx,SqlValue wh)
        {
            return base.AddCondition(cx,wh?.PartsIn(globalFrom.source.rowType));
        }
        internal override TypedValue Eval(Context cx, bool asArray)
        {
            var tr = cx.db;
            long qp = globalFrom.QuerySpec(cx); // can be a From if we are in a join
            var cs = globalFrom.source as CursorSpecification;
            cs.MoveConditions(cx,cs.usingFrom); // probably updates all the queries
            var qs = (Query)cx.obs[qp];
            cs = globalFrom.source as CursorSpecification;
            var uf = cs.usingFrom;
            var usingTable = tr.objects[uf.target] as Table;
            var usingIndex = usingTable.FindPrimaryIndex(tr);
            var ut = tr.role.obinfos[usingTable.defpos] as ObInfo;
            var usingTableColumns = ut.columns;
            var urs = new IndexRowSet(cx, usingTable, usingIndex);
            var rs = BList<(long,TRow)>.Empty;
            for (var b = urs.First(cx); b != null; b = b.Next(cx))
            {
                var ur = b;
                var url = ur[usingTableColumns[(int)usingTableColumns.Count - 1].defpos];
                var sv = new SqlHttp(defpos, globalFrom, new SqlLiteral(-1,url), "application/json", 
                    globalFrom.where, cs.ToString(), ur, globalFrom.matches);
                if (sv.Eval(cx, true) is TArray rv)
                    for (var i = 0; i < rv.Length; i++)
                        rs += (cx.nextHeap++,rv[i] as TRow);
                else
                    return null;
            }
            var ers = new ExplicitRowSet(defpos,cx, cs.rowType.info, rs);
            return ers;
        }
/*        internal override void Delete(Transaction tr,RestView rv, Query f, BTree<string, bool> dr, Adapters eqs)
        {
            var globalFrom = tr.Ctx(blockid) as Query;
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
                var s = new SqlHttp(tr, f, new SqlLiteral(tr, url), "application/json", f.source.domain, Query.PartsIn(where,f.source.domain),"",ur);
                s.Delete(tr,rv, f, dr, eqs);
            }
        }
        internal override void Update(Transaction tr,RestView rv, Query f, BTree<string, bool> ds, Adapters eqs, List<RowSet> rs)
        {
            var globalFrom = tr.Ctx(blockid) as Query;
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
                var s = new SqlHttp(tr, f, new SqlLiteral(tr, url), "application/json", f.source.domain, Query.PartsIn(f.source.where, f.source.domain), "", ur);
                s.Update(tr, rv, f, ds, eqs, rs);
            }
        }
        internal override void Insert(RestView rv, Query f, string prov, RowSet data, Adapters eqs, List<RowSet> rs)
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
                        var rw = a.row;
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
        }*/
    }
}
