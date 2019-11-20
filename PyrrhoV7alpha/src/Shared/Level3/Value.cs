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
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
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
            _From = -306, // From
            Info = -307, // ObInfo
            Left = -308, // SqlTable
            Right = -309, // SqlValue
            Sub = -310; // SqlValue
        public string name => (string)mem[Name]??"";
        internal SqlValue left => (SqlValue)mem[Left];
        internal SqlValue right => (SqlValue)mem[Right];
        internal SqlValue sub => (SqlValue)mem[Sub];
        internal From from => (From)mem[_From];
        internal ObInfo info => (ObInfo)mem[Info];
        public SqlValue(long dp, string nm, BTree<long,object>m=null) 
            : base(dp, (m??BTree<long, object>.Empty)+(Name, nm)) { }
        protected SqlValue(long dp,BTree<long,object> m):base(dp,m)
        { }
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
        /// <summary>
        /// The Export transformer allows things like select * from a view. In simple cases this merely
        /// copies column names from the view. 
        /// But if a SqlValue in the view is COUNT(*) this will not
        /// make sense outside the view. Similar problems arise with table-valued functions.
        /// In such a case Export will replace it with a simple column, with the blockid 
        /// for the GroupRowSet, EvalRowSet, or SqlCall.
        /// </summary>
        /// <param name="tr"></param>
        /// <param name="q">The outer query</param>
        /// <returns></returns>
        internal virtual SqlValue Export(Query q)
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
        internal virtual SqlValue _ColsForRestView(long dp, From gf, GroupSpecification gs,
            ref BTree<SqlValue, string> gfc, ref BTree<SqlValue, string> rem, ref BTree<string, bool?> reg,
            ref BTree<long, SqlValue> map)
        {
            throw new NotImplementedException();
        }
        internal virtual bool Grouped(GroupSpecification gs)
        {
            return gs?.Has(this) == true;
        }
        internal virtual SqlValue Resolve(Context cx,From fm,ref ObInfo ti)
        {
            if (cx.defs.Contains(name))
            {
                var (ob, ns) = cx.defs[name];
                if (ti == null && ob is SqlCol sc)
                    return sc;
                if (ob == ti)
                    return (SqlValue)cx.Replace(this,new SqlTable(ob.defpos, name));
            }
            if (domain != Domain.Content)
                return this;
            if (name==ti?.name)
                return (SqlValue)cx.Replace(this,new SqlTable(ti.defpos, name, fm));
            if (name != "" && (ti?.map.Contains(name)==true))
            {
                var i = ti.map[name].Value;
                var sc = (SqlCol)ti.columns[i];
                var nc = (cx.rawCols)?sc:
                    (SqlValue)cx.Replace(this,new SqlCol(defpos, alias??name, sc.tableCol));
                ti += (ObInfo.Columns, ti.columns + (i, nc));
                return nc;
            }
            return this;
        }
        internal virtual long Defpos()
        {
            return defpos;
        }
        internal virtual Query AddMatches(Query f)
        {
            return f;
        }
        internal virtual DBObject target => null;
        /// <summary>
        /// Get the position of a given expression, allowing for adapter functions
        /// </summary>
        /// <param name="sv">the expression</param>
        /// <returns>the position in the query</returns>
        internal virtual int ColFor(Context cx,Query q)
        {
            return q.rowType.map[name] ?? -1;
        }
        internal override TypedValue Eval(Transaction tr, Context cx)
        {
            return domain.Coerce(cx.values[defpos]);
        }
        internal override TypedValue Eval(Context _cx, RowBookmark bmk)
        {
            return Eval(bmk._rs._tr,_cx); // obviously we aim to do better than this
        }
        internal object Val(Context _cx, RowSet rs)
        {
            return Eval(rs._tr,_cx)?.NotNull()?.Val();
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
        /// Called by GroupingBookmark (when not building) and SelectedRowBookmark
        /// </summary>
        /// <param name="bmk"></param>
        internal virtual void OnRow(RowBookmark bmk)
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
            return false;
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (defpos == so.defpos) ? sv : this;
            var nf = (ObInfo)info?.Replace(cx, so, sv);
            if (nf != info)
                r += (Info, nf);
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
        internal virtual SqlValue PartsIn(ObInfo dt)
        {
            return this;
        }
        internal virtual void BuildRepl(Query lf,ref BTree<SqlValue,SqlValue> lr,ref BTree<SqlValue,SqlValue> rr)
        {
        }
        internal bool Matches(Transaction tr,Context cx)
        {
            return (Eval(tr,cx) is TypedValue tv) ? tv == TBool.True : true;
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
        internal virtual void RowSet(Transaction tr,Context cx,Query f)
        {
            if (f.Eval(tr,cx) is TRow r)
                cx.rb = new TrivialRowSet(tr, cx, f, r).First(cx);
        }
        /// <summary>
        /// Get the position of a given expression, allowing for adapter functions
        /// </summary>
        /// <param name="sv">the expression</param>
        /// <returns>the position in the query</returns>
        internal virtual int? ColFor(Context cx,RowSet rs)
        {
            return rs.qry.rowType.map[name]??-1;
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
        internal virtual void Eqs(Transaction tr,Context cx,ref Adapters eqs)
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
        internal virtual void _AddReqs(Transaction tr,Query gf,Domain ut, ref BTree<SqlValue,int> gfreqs,int i)
        {
            if (from==gf)
                gfreqs +=(this, i);
        }
        internal void AddReqs(Transaction tr, Query gf, Domain ut, ref BTree<SqlValue, int> gfreqs, int i)
        {
            for (var b = gfreqs.First(); b != null; b = b.Next())
                if (MatchExpr(gf, b.key()))
                    return;
            _AddReqs(tr, gf, ut, ref gfreqs, i);
        }
        internal bool IsNeeded(Lexer cx,Query q)
        {
            var qs = q.QuerySpec();
            // check this is being grouped
            bool gp = false;
            bool nogroups = true;
            if (qs?.tableExp?.group is GroupSpecification g)
                for (var gs= g.sets.First();gs!=null;gs=gs.Next())
                {
                    gs.value().Grouped(this, ref gp);
                    nogroups = false;
                }
            return gp || nogroups;
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
            var r = this;
            var d = wr.Fix(defpos);
            if (d != defpos)
                r = (SqlValue)Relocate(d);
            var f = from?.Relocate(wr);
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
        public override string ToString()
        {
            return (prefix==null)?"*":("("+prefix.ToString()+").*");
        }
    }
    internal class SqlTable : SqlValue
    {
        public SqlTable(long dp,string nm,BTree<long,object> m = null)
            : base(dp,(m??BTree<long,object>.Empty)
                  +(Name,nm)+(_Domain,Domain.TableType)) { }
        public SqlTable(long dp, string nm, From fm, BTree<long, object> m = null)
            : this(dp, nm, (m ?? BTree<long, object>.Empty) + (_From, fm)
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
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var fm = (From)from.Relocate(wr);
            if (fm != from)
                r = r + (_From, fm);
            return r;
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
        internal override TypedValue Eval(Transaction tr, Context cx)
        {
            return from?.Eval(tr, cx);
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (from!=null) { sb.Append(" Table:"); sb.Append(from); }
            return sb.ToString();
        }
    }
    internal class SqlCopy : SqlValue
    {
        internal const long
            CopyFrom = -284; // SqlValue
        public SqlValue copyFrom => (SqlValue)mem[CopyFrom];
        public SqlCopy(long dp, SqlValue sc, BTree<long, object> m = null)
            : base(dp, (m ?? BTree<long, object>.Empty) + (Name, sc.name)
                 + (CopyFrom, sc) + (_Domain,sc.domain)) { }
        protected SqlCopy(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCopy operator +(SqlCopy s, (long, object) x)
        {
            return (SqlCopy)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlCopy(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlCopy(dp, mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var co = (SqlValue)copyFrom.Relocate(wr);
            return new SqlCopy(defpos,co,mem);
        }
        internal override SqlValue Resolve(Context cx, From fm, ref ObInfo ti)
        {
            if (domain != Domain.Content)
                return this;
            var sc = cx.obs[copyFrom.defpos];
            if (sc == copyFrom)
                return this;
            var r = this + (_Domain, sc.domain) + (CopyFrom, sc);
            return (SqlValue)Replace(cx, this,r);
        }
        internal override TypedValue Eval(Context _cx, RowBookmark bmk)
        {
            return copyFrom.Eval(_cx, bmk);
        }
        internal override TypedValue Eval(Transaction tr, Context cx)
        {
            return copyFrom.Eval(tr, cx);
        }
        internal override void _AddIn(Context _cx, RowBookmark rb, ref BTree<long, bool?> aggsDone)
        {
            copyFrom._AddIn(_cx, rb, ref aggsDone);
        }
        internal override void StartCounter(Context _cx, RowSet rs)
        {
            copyFrom.StartCounter(_cx, rs);
        }
        internal override DBObject target => copyFrom.target;
        public override string ToString()
        {
            return base.ToString() + " copy from "+Uid(copyFrom.defpos);
        }
    }
    internal class SqlCol : SqlValue
    {
        internal const long
            TableCol = -322; // TableColumn
        public TableColumn tableCol => (TableColumn)mem[TableCol];
        protected SqlCol(long dp, string nm, BTree<long, object> m = null)
            : base(dp, (m ?? BTree<long, object>.Empty) + (Name, nm)) { }
        public SqlCol(long dp, string nm, TableColumn tc, BTree<long, object> m = null)
            : this(dp, nm, (m ?? BTree<long, object>.Empty) + (TableCol, tc)
                  +(_Domain,tc.domain)) { }
        protected SqlCol(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCol operator +(SqlCol s, (long, object) x)
        {
            return (SqlCol)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlCol(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlCol(dp, mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var tc = tableCol.Relocate(wr);
            if (tc != tableCol)
                r += (TableCol, tc);
            return r;
        }
        internal override TypedValue Eval(Context _cx, RowBookmark bmk)
        {
            return _cx.values[tableCol.defpos];
        }
        internal override bool Uses(long t)
        {
            return tableCol?.tabledefpos==t;
        }
        internal override long Defpos()
        {
            return tableCol?.defpos ?? base.Defpos();
        }
        internal override DBObject target => tableCol;
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(TableCol))
            {
                sb.Append(" Col: ");sb.Append(Uid(tableCol.defpos));
            }
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
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            return new TTypeSpec(type);
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
            var r = base.Relocate(wr);
            var t = (Domain)type.Relocate(wr);
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
            var r = base.Relocate(wr);
            var te = (SqlValue)val.Relocate(wr);
            if (te != val)
                r += (TreatExpr, te);
            return r;
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlTreatExpr)base.Replace(cx,so,sv);
            var v = (SqlValue)r.val.Replace(cx,so,sv);
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
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            if (val.Eval(tr,cx)?.NotNull() is TypedValue tv)
            {
                if (!domain.HasValue(tv))
                    throw new DBException("2200G", domain.ToString(), val.ToString()).ISO();
                return tv;
            }
            return null;
        }
        internal override TypedValue Eval(Context _cx, RowBookmark bmk)
        {
            var rs = bmk._rs;
            var tr = rs._tr;
            if (val.Eval(_cx,bmk)?.NotNull() is TypedValue tv)
            {
                if (!domain.HasValue(tv))
                    throw new DBException("2200G", domain.ToString(), val.ToString()).ISO();
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
        internal static readonly long
            Field = -314; // string
        internal string field => (string)mem[Field];
        internal SqlField(long defpos,Domain dt,SqlValue lf,Ident s) 
            : base(defpos,BTree<long,object>.Empty
                  +(_Domain,dt)+(Field,s.ident)+(Left,lf))
        { }
        protected SqlField(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlField operator +(SqlField s, (long, object) x)
        {
            return (SqlField)s.New(s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlField(defpos,m);
        }
        internal override TypedValue Eval(Transaction tr, Context cx)
        {
            return left.Eval(tr,cx)[field];
        }
        internal override TypedValue Eval(Context _cx, RowBookmark bmk)
        {
            return left.Eval(_cx,bmk)[field];
        }
        public override string ToString()
        {
            var sb = new StringBuilder(left.ToString());
            sb.Append(" Field:"); sb.Append(field);
            return sb.ToString();
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlField(dp,mem);
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
            Kind = -315, //Sqlx
            Modifier = -316; // Sqlx
        public Sqlx kind => (Sqlx)mem[Kind];
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
        public SqlValueExpr(long dp, Sqlx op, SqlValue lf, SqlValue rg, Sqlx m,
            BTree<long, object> mm = null)
            : base(dp, (mm ?? BTree<long, object>.Empty) + (_Domain, _Type(dp, op, m, lf, rg))
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
                s += (_Domain, _Type(s.defpos, s.kind, s.mod, s.left, s.right));
            return s;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlValueExpr(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlValueExpr(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var lf = (SqlValue)left?.Relocate(wr);
            var rg = (SqlValue)right?.Relocate(wr);
            return new SqlValueExpr(defpos, kind, lf, rg, mod,mem);
        }
        internal override SqlValue Resolve(Context cx, From fm, ref ObInfo ti)
        {
            var lf = left?.Resolve(cx, fm, ref ti);
            if (kind == Sqlx.DOT && lf.defpos==ti?.defpos && (ti?.map.Contains(right.name)==true))
            {
                var i = ti.map[right.name].Value;
                var sc = (SqlCol)ti.columns[i];
                var nc = (cx.rawCols) ? sc :
                    (SqlValue)cx.Replace(this, new SqlCol(defpos,
                    (left.alias ?? left.name) + "." + (right.alias ?? right.name), sc.tableCol,
                    mem - Left - Right));
                ti += (ObInfo.Columns, ti.columns + (i, nc));
                return nc;
            }
            var rg = right?.Resolve(cx, fm, ref ti);
            if (lf != left || rg != right)
                return (SqlValue)cx.Replace(this,
                    new SqlValueExpr(defpos, kind, lf, rg, mod, mem));
            return this;
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlValueExpr)base.Replace(cx,so,sv);
            var lf = r.left.Replace(cx,so,sv);
            if (lf != r.left)
                r += (Left, lf);
            var rg = (SqlValue)r.right.Replace(cx,so,sv);
            if (rg != r.right)
                r += (Right, rg);
            cx.done += (defpos, r);
            return r;
        }
        internal override DBObject TableRef(Context cx, From f)
        {
            DBObject r = null;
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
        internal override SqlValue Export(Query q)
        {
            if (left.Export(q) is SqlValue a)
            {
                var b = right?.Export(q);
                if (left == a && right == b)
                    return this;
                if (right == null || b != null)
                    return new SqlValueExpr(defpos, kind, a, b, mod) + (_Alias, alias);
            }
            return null;
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
        internal override void _AddReqs(Transaction tr, Query gf, Domain ut, 
            ref BTree<SqlValue, int> gfreqs, int i)
        {
            left.AddReqs(tr, gf, ut, ref gfreqs, i);
            right?.AddReqs(tr, gf, ut, ref gfreqs, i);

        }
        const int ea = 1, eg = 2, la = 4, lr = 8, lg = 16, ra = 32, rr = 64, rg = 128;
        internal override SqlValue _ColsForRestView(long dp,
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
            if (gf.QuerySpec().aggregates())
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
                el = left._ColsForRestView(dp, gf, gs, ref gfl, ref rel, ref rgl, ref map);
            }
            if (right?.IsFrom(gf) == true && (!right.isConstant))
            {
                cse += rr;
                er = right._ColsForRestView(dp, gf, gs, ref gfr, ref rer, ref rgr, ref map);
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
                        return base._ColsForRestView(dp, gf, gs, ref gfc, ref rem, ref reg, ref map);
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
        internal override void _AddIn(Context _cx,RowBookmark rb, ref BTree<long, bool?> aggsDone)
        {
            left?._AddIn(_cx,rb, ref aggsDone);
            right?._AddIn(_cx,rb, ref aggsDone);
        }
        internal override SqlValue SetReg(Context _cx,TRow k)
        {
            left?.SetReg(_cx,k);
            right?.SetReg(_cx,k);
            return this;
        }
        internal override void OnRow(RowBookmark bmk)
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
                                j.left.AddMatch(cx, right, left.Eval(null, cx));
                                return j;
                            }
                            else if (right.IsFrom(j.right, false) && right.defpos > 0)
                            {
                                j.right.AddMatch(cx, right, left.Eval(null, cx));
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
                                j.left.AddMatch(cx, left, right.Eval(null, cx));
                                return j;
                            }
                            else if (left.IsFrom(j.right, false) && left.defpos > 0)
                            {
                                j.right.AddMatch(cx, left, right.Eval(null, cx));
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
        internal override SqlValue PartsIn(ObInfo dt)
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
        internal override TypedValue Eval(Transaction tr, Context cx)
        {
            TypedValue v = null;
            try
            {
                switch (kind)
                {
                    case Sqlx.AND:
                        {
                            var a = left.Eval(tr, cx)?.NotNull();
                            var b = right.Eval(tr, cx)?.NotNull();
                            if (a == null || b == null)
                                return null;
                            if (mod == Sqlx.BINARY) // JavaScript
                                v = new TInt(a.ToLong() & b.ToLong());
                            else
                                v = (a.IsNull || b.IsNull) ?
                                    domain.defaultValue :
                                    TBool.For(((TBool)a).value.Value && ((TBool)b).value.Value);
                            cx.row.values += (defpos, v);
                            return v;
                        }
                    case Sqlx.ASC: // JavaScript ++
                        {
                            v = left.Eval(tr, cx)?.NotNull();
                            if (v == null)
                                return null;
                            if (v.IsNull)
                                return domain.defaultValue;
                            var w = v.dataType.Eval(tr,defpos,v, Sqlx.ADD, new TInt(1L));
                            cx.row.values += (defpos, v);
                            return (mod == Sqlx.BEFORE) ? w : v;
                        }
                    case Sqlx.ASSIGNMENT:
                        {
                            var a = left;
                            var b = right.Eval(tr, cx)?.NotNull();
                            if (b == null)
                                return null;
                            if (a == null)
                                return b;
                            cx.row.values += (a.defpos, v);
                            return v;
                        }
                    case Sqlx.COLLATE:
                        {
                            var a = left.Eval(tr, cx)?.NotNull();
                            object o = a?.Val();
                            if (o == null)
                                return null;
                            Domain ct = left.domain;
                            if (ct.kind == Sqlx.CHAR)
                            {
                                var b = right.Eval(tr, cx)?.NotNull();
                                if (b == null)
                                    return null;
                                string cname = b?.ToString();
                                if (ct.culture.Name == cname)
                                    return left.Eval(tr, cx)?.NotNull();
                                Domain nt = new Domain(defpos,ct.kind, BTree<long, object>.Empty
                                    + (Domain.Precision, ct.prec) + (Domain.Charset, ct.charSet)
                                    + (Domain.Culture, new CultureInfo(cname)));
                                return new TChar(nt, (string)o);
                            }
                            throw new DBException("2H000", "Collate on non-string?").ISO();
                        }
                    case Sqlx.COMMA: // JavaScript
                        {
                            if (left.Eval(tr, cx)?.NotNull() == null)// for side effects
                                return null;
                            return right.Eval(tr, cx);
                        }
                    case Sqlx.CONCATENATE:
                        {
                            if (left.domain.kind == Sqlx.ARRAY
                                && right.domain.kind == Sqlx.ARRAY)
                                return left.domain.Concatenate((TArray)left.Eval(tr, cx),
                                    (TArray)right.Eval(tr, cx));
                            var lf = left.Eval(tr, cx)?.NotNull();
                            var or = right.Eval(tr, cx)?.NotNull();
                            if (lf == null || or == null)
                                return null;
                            var stl = lf.ToString();
                            var str = or.ToString();
                            return new TChar(or.dataType, (lf.IsNull && or.IsNull) ? null : stl + str);
                        }
                    case Sqlx.CONTAINS:
                        {
                            var ta = left.Eval(tr, cx)?.NotNull();
                            if (ta == null)
                                return null;
                            var a = ta.Val() as Period;
                            if (a == null)
                                return domain.defaultValue;
                            if (right.domain.kind == Sqlx.PERIOD)
                            {
                                var tb = right.Eval(tr, cx)?.NotNull();
                                if (tb == null)
                                    return null;
                                var b = tb.Val() as Period;
                                if (b == null)
                                    return TBool.Null;
                                return TBool.For(a.start.CompareTo(b.start) <= 0
                                    && a.end.CompareTo(b.end) >= 0);
                            }
                            var c = right.Eval(tr, cx)?.NotNull();
                            if (c == null)
                                return null;
                            if (c == TNull.Value)
                                return TBool.Null;
                            return TBool.For(a.start.CompareTo(c) <= 0 && a.end.CompareTo(c) >= 0);
                        }
                    case Sqlx.DESC: // JavaScript --
                        {
                            v = left.Eval(tr, cx)?.NotNull();
                            if (v == null)
                                return null;
                            if (v.IsNull)
                                return domain.defaultValue;
                            var w = v.dataType.Eval(tr,defpos,v, Sqlx.MINUS, new TInt(1L));
                            cx.row.values += (defpos, v);
                            return (mod == Sqlx.BEFORE) ? w : v;
                        }
                    case Sqlx.DIVIDE:
                        v = domain.Eval(tr,defpos,left.Eval(tr, cx)?.NotNull(), kind,
                            right.Eval(tr, cx)?.NotNull());
                        if (v != null)
                            cx.row.values += (defpos, v);
                        return v;
                    case Sqlx.DOT:
                        v = left.Eval(tr, cx);
                        if (v != null)
                            v = v[right.name];
                        return v;
                    case Sqlx.EQL:
                        {
                            var rv = right.Eval(tr, cx)?.NotNull();
                            if (rv == null)
                                return null;
                            return TBool.For(rv != null
                                && rv.CompareTo(left.Eval(tr, cx)?.NotNull()) == 0);
                        }
                    case Sqlx.EQUALS:
                        {
                            var ta = left.Eval(tr, cx)?.NotNull();
                            var tb = right.Eval(tr, cx)?.NotNull();
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
                            var ta = left.Eval(tr, cx) as TMultiset;
                            var tb = right.Eval(tr, cx) as TMultiset;
                            if (ta == null || tb == null)
                                return null;
                            return left.domain.Coerce(TMultiset.Except(ta, tb, mod == Sqlx.ALL));
                        }
                    case Sqlx.GEQ:
                        {
                            var ta = left.Eval(tr, cx)?.NotNull();
                            var tb = right.Eval(tr, cx)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return TBool.For(ta.CompareTo(tb) >= 0);
                        }
                    case Sqlx.GTR:
                        {
                            var ta = left.Eval(tr, cx)?.NotNull();
                            var tb = right.Eval(tr, cx)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return TBool.For(ta.CompareTo(tb) > 0);
                        }
                    case Sqlx.INTERSECT:
                        {
                            var ta = left.Eval(tr, cx) as TMultiset;
                            var tb = right.Eval(tr, cx) as TMultiset;
                            if (ta == null || tb == null)
                                return null;
                            return left.domain.Coerce(TMultiset.Intersect(ta, tb, mod == Sqlx.ALL));
                        }
                    case Sqlx.LBRACK:
                        {
                            var al = left.Eval(tr, cx)?.NotNull();
                            var ar = right.Eval(tr, cx)?.NotNull();
                            if (al == null || ar == null)
                                return null;
                            var sr = ar.ToInt();
                            if (al.IsNull || !sr.HasValue)
                                return domain.defaultValue;
                            return ((TArray)al)[sr.Value];
                        }
                    case Sqlx.LEQ:
                        {
                            var ta = left.Eval(tr, cx)?.NotNull();
                            var tb = right.Eval(tr, cx)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return TBool.For(ta.CompareTo(tb) <= 0);
                        }
                    case Sqlx.LOWER: // JavScript >> and >>>
                        {
                            long a;
                            var ol = left.Eval(tr, cx)?.NotNull();
                            var or = right.Eval(tr, cx)?.NotNull();
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
                            cx.row.values += (defpos, v);
                            return v;
                        }
                    case Sqlx.LSS:
                        {
                            var ta = left.Eval(tr, cx)?.NotNull();
                            var tb = right.Eval(tr, cx)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return TBool.For(ta.CompareTo(tb) < 0);
                        }
                    case Sqlx.MINUS:
                        {
                            var tb = right.Eval(tr, cx)?.NotNull();
                            if (tb == null)
                                return null;
                            if (left == null)
                            {
                                v = right.domain.Eval(tr,defpos,new TInt(0), Sqlx.MINUS, tb);
   //                             cx.row.values += (defpos, v);
                                return v;
                            }
                            var ta = left.Eval(tr, cx)?.NotNull();
                            if (ta == null)
                                return null;
                            v = left.domain.Eval(tr,defpos,ta, kind, tb);
                            cx.row.values += (defpos, v);
                            return v;
                        }
                    case Sqlx.NEQ:
                        {
                            var rv = right.Eval(tr, cx)?.NotNull();
                            return TBool.For(left.Eval(tr, cx)?.NotNull().CompareTo(rv) != 0);
                        }
                    case Sqlx.NO: return left.Eval(tr, cx);
                    case Sqlx.NOT:
                        {
                            var ta = left.Eval(tr, cx)?.NotNull();
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
                            var ta = left.Eval(tr, cx)?.NotNull();
                            var tb = right.Eval(tr, cx)?.NotNull();
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
                            cx.row.values += (defpos, v);
                            return v;
                        }
                    case Sqlx.OVERLAPS:
                        {
                            var ta = left.Eval(tr, cx)?.NotNull();
                            var tb = right.Eval(tr, cx)?.NotNull();
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
                            var ta = left.Eval(tr, cx)?.NotNull();
                            var tb = right.Eval(tr, cx)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return new TPeriod(Domain.Period, new Period(ta, tb));
                        }
                    case Sqlx.PLUS:
                        {
                            var ta = left.Eval(tr, cx)?.NotNull();
                            var tb = right.Eval(tr, cx)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            v = left.domain.Eval(tr,defpos,ta, kind, tb);
                            cx.values += (defpos, v);
                            cx.row.values += (defpos, v);
                            return v;
                        }
                    case Sqlx.PRECEDES:
                        {
                            var ta = left.Eval(tr, cx)?.NotNull();
                            var tb = right.Eval(tr, cx)?.NotNull();
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
                    case Sqlx.QMARK:
                        {
                            var a = right as SqlValueExpr;
                            var lf = left.Eval(tr, cx)?.NotNull();
                            if (lf == null)
                                return null;
                            if (lf.IsNull)
                                return domain.defaultValue;
                            var b = ((bool)lf.Val()) ? a.left : a.right;
                            return a.left.domain.Coerce(b.Eval(tr, cx));
                        }
                    case Sqlx.RBRACK:
                        {
                            var a = left.Eval(tr, cx)?.NotNull();
                            var b = right.Eval(tr, cx)?.NotNull();
                            if (a == null || b == null)
                                return null;
                            if (a.IsNull || b.IsNull)
                                return domain.defaultValue;
                            return ((TArray)a)[b.ToInt().Value];
                        }
                    case Sqlx.SUCCEEDS:
                        {
                            var ta = left.Eval(tr, cx)?.NotNull();
                            var tb = right.Eval(tr, cx)?.NotNull();
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
                            var ta = left.Eval(tr, cx)?.NotNull();
                            var tb = right.Eval(tr, cx)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            v = domain.Eval(tr,defpos,ta, kind, tb);
                            cx.row.values += (defpos, v);
                            return v;
                        }
                    case Sqlx.UNION:
                        {
                            var ta = left.Eval(tr, cx)?.NotNull();
                            var tb = right.Eval(tr, cx)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return left.domain.Coerce(
                                TMultiset.Union((TMultiset)ta, (TMultiset)tb, mod == Sqlx.ALL));
                        }
                    case Sqlx.UPPER: // JavaScript <<
                        {
                            var lf = left.Eval(tr, cx)?.NotNull();
                            var or = right.Eval(tr, cx)?.NotNull();
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
                            cx.row.values += (defpos, v);
                            return v;
                        }
                    //       case Sqlx.XMLATTRIBUTES:
                    //         return new TypedValue(left.domain, BuildXml(left) + " " + BuildXml(right));
                    case Sqlx.XMLCONCAT:
                        {
                            var ta = left.Eval(tr, cx)?.NotNull();
                            var tb = right.Eval(tr, cx)?.NotNull();
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
        static Domain _Type(long dp, Sqlx kind, Sqlx mod, SqlValue left, SqlValue right)
        {
            switch (kind)
            {
                case Sqlx.AND:
                    if (mod == Sqlx.BINARY) break; //JavaScript
                    return Domain.Bool;
                case Sqlx.ASC: goto case Sqlx.PLUS; // JavaScript
                case Sqlx.ASSIGNMENT: return right.domain;
                case Sqlx.COLLATE: return Domain.Char;
                case Sqlx.COLON: return left.domain; // JavaScript
                case Sqlx.CONCATENATE: return Domain.Char;
                case Sqlx.DESC: goto case Sqlx.PLUS; // JavaScript
                case Sqlx.DIVIDE:
                    {
                        var dl = left.domain.kind;
                        var dr = right.domain.kind;
                        if (dl == Sqlx.INTERVAL && (dr == Sqlx.INTEGER || dr == Sqlx.NUMERIC))
                            return left.domain;
                        return left.FindType(Domain.UnionNumeric);
                    }
                case Sqlx.DOT: return right.domain;
                case Sqlx.EQL: return Domain.Bool;
                case Sqlx.EXCEPT: return left.domain;
                case Sqlx.GTR: return Domain.Bool;
                case Sqlx.INTERSECT: return left.domain;
                case Sqlx.LOWER: return Domain.Int; // JavaScript >> and >>>
                case Sqlx.LSS: return Domain.Bool;
                case Sqlx.MINUS:
                    if (left != null)
                    {
                        var dl = left.domain.kind;
                        var dr = right.domain.kind;
                        if (dl == Sqlx.DATE || dl == Sqlx.TIMESTAMP || dl == Sqlx.TIME)
                        {
                            if (dr == dl)
                                return Domain.Interval;
                            if (dr == Sqlx.INTERVAL)
                                return left.domain;
                        }
                        else if (dl == Sqlx.INTERVAL && (dr == Sqlx.DATE || dl == Sqlx.TIMESTAMP || dl == Sqlx.TIME))
                            return right.domain;
                        return left.FindType(Domain.UnionDateNumeric);
                    }
                    return right.FindType(Domain.UnionDateNumeric);
                case Sqlx.NEQ: return Domain.Bool;
                case Sqlx.LEQ: return Domain.Bool;
                case Sqlx.GEQ: return Domain.Bool;
                case Sqlx.NO: return left.domain;
                case Sqlx.NOT: goto case Sqlx.AND;
                case Sqlx.OR: goto case Sqlx.AND;
                case Sqlx.PLUS:
                    {
                        var dl = left.domain.kind;
                        var dr = right.domain.kind;
                        if ((dl == Sqlx.DATE || dl == Sqlx.TIMESTAMP || dl == Sqlx.TIME) && dr == Sqlx.INTERVAL)
                            return left.domain;
                        else if (dl == Sqlx.INTERVAL && (dr == Sqlx.DATE || dl == Sqlx.TIMESTAMP || dl == Sqlx.TIME))
                            return right.domain;
                        return left.FindType(Domain.UnionDateNumeric);
                    }
                case Sqlx.QMARK:
                    { // JavaScript
                        var r = right as SqlValueExpr;
                        return r.domain;
                    }
                case Sqlx.RBRACK: return new Domain(dp,Sqlx.ARRAY, left.domain);
                case Sqlx.SET: return left.domain; // JavaScript
                case Sqlx.TIMES:
                    {
                        var dl = left.domain.kind;
                        var dr = right.domain.kind;
                        if (dl == Sqlx.NUMERIC || dr == Sqlx.NUMERIC)
                            return Domain.Numeric;
                        if (dl == Sqlx.INTERVAL && (dr == Sqlx.INTEGER || dr == Sqlx.NUMERIC))
                            return left.domain;
                        if (dr == Sqlx.INTERVAL && (dl == Sqlx.INTEGER || dl == Sqlx.NUMERIC))
                            return right.domain;
                        return left.FindType(Domain.UnionNumeric);
                    }
                case Sqlx.UNION: return left.domain;
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
                            q.AddMatch(cx, left, right.Eval(null, cx));
                            move = true;
                            return q;
                        }
                        else if (left.isConstant && right.IsFrom(q, false) && right.defpos > 0)
                        {
                            q.AddMatch(cx, right, left.Eval(null, cx));
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
        internal override TypedValue Eval(Transaction tr,Context cx)
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
        /// <summary>
        /// Constructor: a Literal
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="ty">the kind of literal</param>
        /// <param name="v">the value of the literal</param>
        public SqlLiteral(long dp, TypedValue v) : base(dp, BTree<long, object>.Empty
            + (_Domain, v.dataType) + (_Val, v))
        { }
        public SqlLiteral(long dp, BTree<long, object> m) : base(dp, m) { }
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
        internal override TypedValue Eval(Transaction tr,Context cx)
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
    internal class SqlTableRowStart : SqlValue
    {
        internal const long
            TableRow = -311; // TableRow 
        internal TableRow rec => (TableRow)mem[TableRow];
        public SqlTableRowStart(long dp, From f, TableRow r) : base(dp,BTree<long,object>.Empty
            +(_Domain,f.domain)+(_From,f)+(TableRow,r))
        { }
        protected SqlTableRowStart(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlTableRowStart operator+(SqlTableRowStart s,(long,object)x)
        {
            return new SqlTableRowStart(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlTableRowStart(defpos, m);
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
        public SqlRow(long dp, BList<SqlValue> vs)
            : base(dp, BTree<long, object>.Empty
                  + (Info, new ObInfo(dp,Domain.Row,vs)) + (Dependents, _Deps(vs)) 
                  + (Depth, 1 + _Depth(vs)))
        { }
        public static SqlRow operator+(SqlRow s,(long,object)m)
        {
            return new SqlRow(s.defpos, s.mem + m);
        }
        public static SqlRow operator+(SqlRow s,SqlValue v)
        {
            return new SqlRow(s.defpos, s.info.columns + v);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlRow(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlRow(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var cs = BList<SqlValue>.Empty;
            var ch = false;
            for (var b=info.columns.First();b!=null;b=b.Next())
            {
                var c = (SqlValue)b.value().Relocate(wr);
                ch = ch || (c != b.value());
                cs += c;
            }
            if (ch)
                r += (Info, new ObInfo(defpos,domain,cs));
            return r;
        }
        internal DBObject this[int i] =>info.columns[i];
        internal int Length => (int)info.columns.Count;
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlRow)base.Replace(cx,so,sv);
            var cs = r.info.columns;
            for (var b=cs.First();b!=null;b=b.Next())
            {
                var v = (SqlValue)b.value().Replace(cx,so,sv);
                if (v != b.value())
                    cs += (b.key(), v);
            }
            if (cs != r.info.columns)
                r += (Info, new ObInfo(defpos,domain,cs));
            cx.done += (defpos, r);
            return r;
        }
        internal override bool Uses(long t)
        {
            for (var b = info.columns.First(); b != null; b = b.Next())
                if (b.value().Uses(t))
                    return true;
            return false;
        }
        internal override SqlValue Resolve(Context cx, From fm, ref ObInfo ti)
        {
            var cs = BList<SqlValue>.Empty;
            var ch = false;
            for (var b = info.columns.First(); b != null; b = b.Next())
            {
                var v = b.value().Resolve(cx, fm, ref ti);
                cs += v;
                if (v != b.value())
                    ch = true;
            }
            return ch ? (SqlValue)cx.Replace(this,
                this + (Info, new ObInfo(info.defpos,Domain.Row,cs))) : this;
        }
        /// <summary>
        /// the value
        /// </summary>
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            var r = new TypedValue[info.Length];
            for (int i = 0; i < info.Length; i++)
                if (info.columns[i].domain.Coerce(this[i].Eval(tr,cx)?.NotNull()) 
                    is TypedValue v)
                    r[i] = v;
                else
                    return null;
            return new TRow(info, r);
        }
        internal override bool aggregates()
        {
            for (var i = 0; i < info.columns.Count; i++)
                if (info.columns[i].aggregates())
                    return true;
            return false;
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            for (var i = 0; i < info.columns.Count; i++)
                info.columns[i].Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx,RowSet rs)
        {
            for (var i = 0; i < info.Length; i++)
                info.columns[i].StartCounter(_cx,rs);
        }
        internal override void _AddIn(Context _cx,RowBookmark rb, ref BTree<long, bool?> aggsDone)
        {
            for (var i = 0; i < info.columns.Count; i++)
                info.columns[i]._AddIn(_cx,rb, ref aggsDone);
        }
        internal override SqlValue SetReg(Context _cx,TRow k)
        {
            var nulls = true;
            for (var i = 0; i < info.columns.Count; i++)
                if (info.columns[i].SetReg(_cx,k) != null)
                    nulls = false;
            return nulls ? null : this;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = "(";
            for (var i=0;i<Length;i++)
            {
                var c = info.columns[i];
                sb.Append(cm); cm = ",";
                sb.Append(info.columns[i].defpos + "=");sb.Append(info.columns[i]);
            }
            sb.Append(")");
            return sb.ToString();
        }
    }
    internal class SqlRowArray : SqlValue
    {
        internal static readonly long
            Rows = -319; // BList<SqlRow>
        internal BList<SqlRow> rows =>
            (BList<SqlRow>)mem[Rows]?? BList<SqlRow>.Empty;
        public SqlRowArray(long dp, BList<SqlRow> rs) : base(dp, BTree<long, object>.Empty
             + (Rows, rs))
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlRowArray)base.Replace(cx,so,sv);
            var rws = r.rows;
            for (var b=r.rows?.First();b!=null;b=b.Next())
            {
                var v = (SqlValue)b.value().Replace(cx,so,sv);
                if (v != b.value())
                    rws += (b.key(), (SqlRow)v);
            }
            if (rws != r.rows)
                r += (Rows, rws);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool Uses(long t)
        {
            for (var b = rows.First(); b != null; b = b.Next())
                if (b.value().Uses(t))
                    return true;
            return false;
        }
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            var r = new TArray(domain, (int)rows.Count);
            for (var j = 0; j < rows.Count; j++)
                r[j] = rows[j].Eval(tr,cx);
            return r;
        }
        internal override void RowSet(Transaction tr, Context cx, Query f)
        {
            var r = new ExplicitRowSet(tr, cx, f);
            for (var b = rows.First(); b != null; b = b.Next())
            {
                var v = b.value();
                r.Add((v.defpos,(TRow)v.Eval(tr, cx)));
            }
            cx.data = r;
            cx.rb = r.First(cx);
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
        internal override void _AddIn(Context _cx,RowBookmark rb, ref BTree<long, bool?> aggsDone)
        {
            for (var i = 0; i < rows.Count; i++)
                rows[i]._AddIn(_cx,rb, ref aggsDone);
        }
        internal override SqlValue SetReg(Context _cx,TRow k)
        {
            var nulls = true;
            for (var i = 0; i < rows.Count; i++)
                if (rows[i].SetReg(_cx,k) != null)
                    nulls = false;
            return nulls ? null : this;
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
        internal GroupRow(Context _cx, long dp, GroupingRowSet.GroupInfo gi,
            GroupingRowSet.GroupingBookmark gb, ObInfo gt, TRow key)
            : base(dp, BTree<long, object>.Empty
                  + (GroupMap, gi.group.members) + (Info, gt)
                  + (GroupInfo, _Columns(_cx,gi,gb, key)))
        {
            gb._grs.g_rows += (key, this);
        }
        static BList<SqlValue> _Columns(Context _cx, GroupingRowSet.GroupInfo gi,
            GroupingRowSet.GroupingBookmark gb,TRow key)
        {
            var dt = ((GroupingRowSet)gb._rs).qry.rowType;
            var columns = BList<SqlValue>.Empty;
            for (int j = 0; j < dt.Length; j++) // not grs.rowType!
            {
                var c = dt.columns[j];
                var sc = gi.grs.qry.rowType.columns[j];
                columns += (key[c.defpos] is TypedValue tv) ? new SqlLiteral(c.defpos,tv)
                        : sc.SetReg(_cx,key) ?? new SqlLiteral(-1,_cx.values[sc.defpos] ?? TNull.Value);
            }
            return columns;
/*            for (var b = gi.grs.having.First(); b != null; b = b.Next())
                b.value().AddIn(gi.grs); */
        }
        /// <summary>
        /// the value
        /// </summary>
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            var r = new TypedValue[info.columns.Count];
            for (int i = 0; i < r.Length; i++)
                if (info.columns[i].domain.Coerce(this[i].Eval(tr,cx)?.NotNull()) 
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlXmlValue)base.Replace(cx, so, sv);
            var at = r.attrs;
            for (var b=at?.First();b!=null;b=b.Next())
            {
                var v = (SqlValue)b.value().Item2.Replace(cx,so,sv);
                if (v != b.value().Item2)
                    at += (b.key(), (b.value().Item1, v));
            }
            if (at != r.attrs)
                r += (Attrs, at);
            var co = (SqlValue)r.content.Replace(cx,so,sv);
            if (co != r.content)
                r += (Content, co);
            var ch = r.children;
            for(var b=ch?.First();b!=null;b=b.Next())
            {
                var v = (SqlValue)b.value().Replace(cx,so,sv);
                if (v != b.value())
                    ch += (b.key(), (SqlXmlValue)v);
            }
            if (ch != r.children)
                r += (Children, ch);
            cx.done += (defpos, r);
            return r;
        }
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            var r = new TXml(element.ToString());
            for(var b=attrs?.First();b!=null;b=b.Next())
                if (b.value().Item2.Eval(tr,cx)?.NotNull() is TypedValue ta)
                    r.attributes+=(b.value().Item1.ToString(), ta);
            for(var b=children?.First();b!=null;b=b.Next())
                if (b.value().Eval(tr,cx) is TypedValue tc)
                    r.children+=(TXml)tc;
            if (content?.Eval(tr,cx)?.NotNull() is TypedValue tv)
                r.content= (tv as TChar)?.value;
            return r;
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlSelectArray)base.Replace(cx,so,sv);
            var ae = r.aqe.Replace(cx,so,sv);
            if (ae != r.aqe)
                r += (ArrayValuedQE, ae);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool Uses(long t)
        {
            return aqe.Uses(t);
        }
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            var va = new TArray(domain);
            var et = (ObInfo)tr.role.obinfos[domain.elType.defpos]; //??
            var ars = aqe.RowSets(tr,cx);
            int j = 0;
            var nm = aqe.name;
            for (var rb=ars.First(cx);rb!= null;rb=rb.Next(cx))
            {
                var rw = rb.row;
                if (et==null)
                    va[j++] = rw[nm];
                else
                {
                    var vs = new TypedValue[aqe.display];
                    for (var i = 0; i < aqe.display; i++)
                        vs[i] = rw[i];
                    va[j++] = new TRow(et, vs);
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
        internal override void _AddIn(Context _cx,RowBookmark rb, ref BTree<long, bool?> aggsDone)
        {
            aqe.AddIn(_cx,rb);
        }
        internal override SqlValue SetReg(Context _cx,TRow k)
        {
            aqe.SetReg(_cx,k);
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
        internal override DBObject Relocate(long dp)
        {
            return new SqlValueArray(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlValueArray)base.Replace(cx,so,sv);
            var ar = r.array;
            for (var b=ar?.First();b!=null;b=b.Next())
            {
                var v = (SqlValue)b.value().Replace(cx,so,sv);
                if (v != b.value())
                    ar += (b.key(), v);
            }
            if (ar != r.array)
                r += (Array, ar);
            var ss = (SqlValue)r.svs?.Replace(cx,so,sv);
            if (ss != r.svs)
                r += (Svs, ss);
            cx.done += (defpos, r);
            return r;
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
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            if (svs != null)
            {
                var ar = new List<TypedValue>();
                var ers = svs?.Eval(tr, cx) as TRowSet;
                for (var b = ers.rowSet.First(cx); b != null; b = b.Next(cx))
                    ar.Add(b.row[0]);
                return new TArray(domain, ar);
            }
            var a = new TArray(domain,(int)array.Count);
            for (int j = 0; j < (array?.Count??0); j++)
                a[j] = array[j]?.Eval(tr,cx)?.NotNull() ?? domain.defaultValue;
            return a;
        }
        internal override TypedValue Eval(Context _cx, RowBookmark bmk)
        {
            var rs = bmk._rs;
            var tr = rs._tr;
            if (svs != null)
            {
                var ar = new List<TypedValue>();
                var ers = svs?.Eval(_cx,bmk) as TRowSet;
                for (var b = ers?.rowSet.First(_cx); b != null; b = b.Next(_cx))
                    ar.Add(b.row[0]);
                return new TArray(domain, ar);
            }
            var a = new TArray(domain, (int)array.Count);
            for (int j = 0; j < (array?.Count??0); j++)
                a[j] = array[j]?.Eval(_cx,bmk)?.NotNull() ?? domain.defaultValue;
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
        internal override void _AddIn(Context _cx,RowBookmark rb, ref BTree<long, bool?> aggsDone)
        {
            if (array != null)
                for (var i = 0; i < array.Count; i++)
                    array[i]._AddIn(_cx,rb,ref aggsDone);
            svs?.AddIn(_cx,rb);
            base._AddIn(_cx,rb, ref aggsDone);
        }
        internal override SqlValue SetReg(Context _cx,TRow k)
        {
            var nulls = true;
            if (array != null)
                for (var i = 0; i < array.Count; i++)
                    if (array[i].SetReg(_cx,k) != null)
                        nulls = false;
            if (svs?.SetReg(_cx,k) != null)
                nulls = false;
            return nulls ? null : this;
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
            Source = -331, // string
            TargetType = -332; // Domain
        /// <summary>
        /// the subquery
        /// </summary>
        public Query expr =>(Query)mem[Expr];
        public Domain targetType => (Domain)mem[TargetType];
        public string source => (string)mem[Source];
        public SqlValueSelect(long dp, Query q, string s)
            : base(dp,BTree<long,object>.Empty
                  +(Expr,q)+(Source,s))
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
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var e = (Query)expr.Relocate(wr);
            if (e != expr)
                r += (Expr, e);
            var dt = (Domain)targetType.Relocate(wr);
            if (dt != targetType)
                r += (TargetType, dt);
            return r;
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlValueSelect)base.Replace(cx,so,sv);
            var ex = r.expr.Replace(cx,so,sv);
            if (ex != r.expr)
                r += (Expr, ex);
            cx.done += (defpos, r);
            return r;
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
        internal override TypedValue Eval(Transaction tr, Context cx)
        {
            var ers = expr.RowSets(tr,cx);
            if (targetType?.kind == Sqlx.TABLE)
                return new TRowSet(ers ?? EmptyRowSet.Value);
            var rb = ers.First(cx);
            if (rb == null)
                return domain.defaultValue;
            TypedValue tv = rb._rs.qry.rowType.columns[0].Eval(tr,cx)?.NotNull();
            if (targetType != null)
                tv = targetType.Coerce(tv);
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
        internal override void RowSet(Transaction tr, Context cx, Query f)
        {
            cx.rb = expr.RowSets(tr,cx).First(cx);
        }
        internal override void StartCounter(Context _cx, RowSet rs)
        {
            expr.StartCounter(_cx,rs);
        }
        internal override void _AddIn(Context _cx,RowBookmark rb, ref BTree<long, bool?> aggsDone)
        {
            expr.AddIn(_cx,rb);
        }
        internal override SqlValue SetReg(Context _cx,TRow k)
        {
            expr.SetReg(_cx,k);
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
        internal override TypedValue Eval(Context _cx, RowBookmark rb)
        {
            if (rb is GroupingRowSet.GroupingBookmark gb)
            {
                var gp = gb.Grouping();
                for (var b = bits.First(); b != null; b = b.Next())
                    if (gp[b.key()]!=null)
                        return new TInt(0);
                return new TInt(1);
            }
            return base.Eval(_cx,rb);
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
                  (_Domain,cs.domain)+(Name, n)
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
            var r = base.Relocate(wr);
            var sp = (CursorSpecification)spec.Relocate(wr);
            if (sp != spec)
                r += (Spec, sp);
            return r;
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlCursor)base.Replace(cx,so,sv);
            var sp = r.spec.Replace(cx,so,sv);
            if (sp != r.spec)
                r += (Spec, sp);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool Uses(long t)
        {
            return spec.Uses(t);
        }
        internal override TypedValue Eval(Context _cx, RowBookmark rb) 
        {
            return new TCursor(rb);
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
                  +(Depth,1+c.depth))
        { }
        protected SqlCall(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCall operator+(SqlCall c,(long,object)x)
        {
            return new SqlCall(c.defpos, c.mem + x);
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
            var r = base.Relocate(wr);
            var c = (CallStatement)call.Relocate(wr);
            if (c != call)
                r += (Call, c);
            return r;
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlCall)base.Replace(cx,so,sv);
            var ca = r.call.Replace(cx,so,sv);
            if (ca != r.call)
                r += (Call, ca);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool Uses(long t)
        {
            return call.proc.Uses(t);
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
            var mp = rs.qry.rowType.map;
            for (var i = 0; i < call.parms.Count; i++)
                if (mp.Contains((call.parms[i]).name))
                    call.parms[i].Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx, RowSet rs)
        {
            var mp = rs.qry.rowType.map;
            for (var i = 0; i < call.parms.Count; i++)
                if (mp.Contains((call.parms[i]).name))
                    call.parms[i].StartCounter(_cx, rs);
        }
        internal override void _AddIn(Context _cx,RowBookmark rb, ref BTree<long, bool?> aggsDone)
        {
            var mp = rb._rs.qry.rowType.map;
            for (var i = 0; i < call.parms.Count; i++)
                if (mp.Contains((call.parms[i]).name))
                    call.parms[i]._AddIn(_cx,rb, ref aggsDone);
        }
        internal override SqlValue SetReg(Context _cx,TRow k)
        {
            var nulls = true;
            for (var i = 0; i < call.parms.Count; i++)
                if (call.parms[i].SetReg(_cx,k) != null)
                    nulls = false;
            return nulls ? null : this;
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
        /// <summary>
        /// evaluate the procedure call
        /// </summary>
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            try
            {
                call.proc.Exec(tr, cx, call.parms);
                return (cx.row is TypedValue tv)?
                    ((tv != TNull.Value)?tv:domain.defaultValue):null;
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
        internal override void Eqs(Transaction tr,Context cx,ref Adapters eqs)
        {
            if (tr.objects[call.proc.inverse] is Procedure inv)
                eqs = eqs.Add(call.proc.defpos, call.parms[0].defpos, call.proc.defpos, inv.defpos);
            base.Eqs(tr,cx,ref eqs);
        }
        internal override int ColFor(Context cx,Query q)
        {
            if (call.parms.Count == 1)
                return ((SqlValue)call.parms[0]).ColFor(cx,q);
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
        /// <summary>
        /// Evaluate the method call and return the result
        /// </summary>
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            if (call.var == null)
                throw new PEException("PE241");
            return (((Method)call.proc).Exec(tr, cx, call.var, call.parms) is TypedValue tv) ?
                ((tv != TNull.Value) ? tv : domain.defaultValue):null;
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
            Udt = -337; // UDType
        /// <summary>
        /// the type
        /// </summary>
        public UDType ut =>(UDType)mem[Udt];
        public SqlRow sce =>(SqlRow)mem[Sce];
        /// <summary>
        /// set up the Constructor SqlValue
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="u">the type</param>
        /// <param name="c">the call statement</param>
        public SqlConstructor(long dp, UDType u, CallStatement c)
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
            var r = base.Relocate(wr);
            var s = (SqlRow)sce.Relocate(wr);
            if (s != sce)
                r += (Sce, s);
            var u = (UDType)ut.Relocate(wr);
            if (u != ut)
                r += (Udt, u);
            return r;
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlConstructor)base.Replace(cx,so,sv);
            var sc = r.sce.Replace(cx,so,sv);
            if (sc != r.sce)
                r += (Sce, sc);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool Uses(long t)
        {
            return ut.tabledefpos==t;
        }
        /// <summary>
        /// evaluate the constructor and return the new object
        /// </summary>
        internal override TypedValue Eval(Transaction tr, Context cx)
        {
            return (((Method)call.proc).Exec(tr, cx,null,call.parms) is TypedValue tv) ?
                ((tv != TNull.Value) ? tv : domain.defaultValue) : null;
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
        public UDType ut=>(UDType)mem[SqlConstructor.Udt];
        public SqlRow sce=>(SqlRow)mem[SqlConstructor.Sce];
        /// <summary>
        /// construct a SqlValue default constructor for a type
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="u">the type</param>
        /// <param name="lk">the actual parameters</param>
        public SqlDefaultConstructor(long dp, UDType u, BList<SqlValue> ins)
            : base(dp, BTree<long, object>.Empty+(SqlConstructor.Udt, u)
                  +(SqlConstructor.Sce,new SqlRow(dp,ins))
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlDefaultConstructor)base.Replace(cx,so,sv);
            var sc = r.sce.Replace(cx,so,sv);
            if (sc != r.sce)
                r += (SqlConstructor.Sce, sc);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool Uses(long t)
        {
            return ut.tabledefpos ==t;
        }
        /// <summary>
        /// Evaluate the default constructor
        /// </summary>
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            var dt = ut;
            try
            {
                var oi = tr.role.obinfos[dt.defpos] as ObInfo;
                var obs = BTree<long,TypedValue>.Empty;
                for (var b=oi.columns.First();b!=null;b=b.Next())
                    obs += (b.value().defpos,sce[b.key()].Eval(tr,cx));
                return new TRow(ut,oi,obs);
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
            Kind = -339, // Sqlx
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
        internal Query query =>(Query)mem[Query];
        public Sqlx kind => (Sqlx)mem[Kind];
        /// <summary>
        /// A modifier for the function from the parser
        /// </summary>
        public Sqlx mod=>(Sqlx)mem[Mod];
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
        public long windowId => (long)mem[WindowId];
        /// <summary>
        /// the window for a window function
        /// </summary>
        public WindowSpecification window => (WindowSpecification)mem[Window];
        /// <summary>
        /// Check for monotonic
        /// </summary>
        public bool monotonic => (bool)(mem[Monotonic]??false);
        /// <summary>
        /// Constructor: a function SqlValue from the parser
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="f">the function name</param>
        public SqlFunction(long dp, Sqlx f, SqlValue vl, SqlValue o1, SqlValue o2, Sqlx m, 
            BTree<long,object>mm=null) : 
            base(dp,(mm??BTree<long,object>.Empty)+(_Domain,_Type(f,vl,o1))
                +(Name,f.ToString())+(Kind,f)+(Mod,m)+(_Val,vl)+(Op1,o1)+(Op2,o2)
                +(Dependents,_Deps(vl,o1,o2)) +(Depth,_Depth(vl,o1,o2)))
        { }
        protected SqlFunction(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlFunction operator+(SqlFunction s,(long,object)x)
        {
            return new SqlFunction(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlFunction(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new SqlFunction(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var f = (SqlValue)filter.Relocate(wr);
            if (f != filter)
                r += (Filter, f);
            var op = (SqlValue)op1.Relocate(wr);
            if (op != op1)
                r += (Op1, op);
            op = (SqlValue)op2.Relocate(wr);
            if (op != op2)
                r += (Op2, op);
            var v = (SqlValue)val.Relocate(wr);
            if (v != val)
                r += (_Val, v);
            var w = (WindowSpecification)window.Relocate(wr);
            if (w != window)
                r += (Window, w);
            var wi = wr.Fix(windowId);
            if (wi != windowId)
                r += (WindowId, wi);
            return r;
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
        internal override SqlValue Resolve(Context cx, From fm, ref ObInfo ti)
        {
            var vl = val?.Resolve(cx, fm, ref ti);
            var o1 = op1?.Resolve(cx, fm, ref ti);
            var o2 = op2?.Resolve(cx, fm, ref ti);
            if (vl != val || o1 != op1 || o2 != op2)
                return (SqlValue)cx.Replace(this,
                    new SqlFunction(defpos, kind, vl, o1, o2, mod, mem));
            return this;
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlFunction)base.Replace(cx,so,sv);
            var fi = r.filter?.Replace(cx,so,sv);
            if (fi != r.filter)
                r += (Filter, fi);
            var o1 = r.op1?.Replace(cx,so,sv);
            if (o1 != r.op1)
                r += (Op1, o1);
            var o2 = r.op2?.Replace(cx,so,sv);
            if (o2 != r.op2)
                r += (Op2, o2);
            var vl = r.val?.Replace(cx,so,sv);
            if (vl != r.val)
                r += (_Val, vl);
            var qr = r.query?.Replace(cx, so, sv);
            if (qr != r.query)
                r += (Query, qr);
            cx.done += (defpos, r);
            return r;
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
        internal SqlFunction RowSets(Transaction tr, Query q)
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
            fd.regs = new CTree<TRow, FunctionData.Register>(
                new Domain(window.defpos,Sqlx.ROW,BTree<long,object>.Empty));
            for (var b = rs.First(_cx);b!=null;b=b.Next(_cx))
            {
                PRow ks = null;
                for (var i = window.partition - 1;i>=0; i--)
                    ks = new PRow(window.order.items[i].Eval(_cx,b),ks);
                var pkey = new TRow(window.partitionType, ks);
                fd.cur = fd.regs[pkey];
                ks = null;
                for (var i = (int)window.order.items.Count - 1; i >= window.partition; i--)
                    ks = new PRow(window.order.items[i].Eval(_cx,b), ks);
                var okey = new TRow(window.partitionType, ks);
                var worder = OrderSpec.Empty;
                var its = BTree<int, DBObject>.Empty;
                for (var i = window.partition; i < window.order.items.Count; i++)
                    its+=((int)its.Count,window.order.items[i]);
                worder = worder+ (OrderSpec.Items, its);
                if (fd.cur ==null)
                {
                    fd.cur = new FunctionData.Register();
                    fd.regs+=(pkey, fd.cur);
                    fd.cur.wrs = new OrderedRowSet(_cx,rs.qry, rs, worder, false);
                    fd.cur.wrs.tree = new RTree(rs, 
                        new TreeInfo(window.partitionType, TreeBehaviour.Allow, TreeBehaviour.Allow));
                }
                var dt = rs.qry.rowType;
                var vs = new TypedValue[dt.Length];
                for (var i = 0; i < dt.Length; i++)
                    vs[i] = dt.columns[i].Eval(_cx,b);
                fd.cur.wrs.tree+=(okey,new TRow(dt,vs));
            }
            fd.building = false;
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
        internal override TypedValue Eval(Transaction tr, Context cx)
        {
            var rb = cx.rb;
            var fd = cx.func[defpos];
            if (fd==null)
                cx.func += (defpos, fd = new FunctionData());
            RowBookmark firstTie = null;
            if (rb==null || fd.building == true)
                return null;
            /*          if (query is Query q0 && tr.Ctx(q0.blockid) is Query q 
                          && ((q.rowSet is GroupingRowSet g && !g.building 
                          && g.g_rows == null) || q.rowSet is ExportedRowSet))
                              return q.row?.Get(alias ?? name); */
            if (window != null)
            {
                if (fd.valueInProgress)
                    return null;
                fd.valueInProgress = true;
                PRow ks = null;
                for (var i = window.partition - 1; i >= 0; i--)
                    ks = new PRow(window.order.items[i].Eval(cx, rb), ks);
                fd.cur = fd.regs[new TRow(window.partitionType, ks)];
                fd.cur.Clear();
                fd.cur.wrs.building = false; // ? why should it be different?
                ks = null;
                for (var i = (int)window.order.items.Count - 1; i >= window.partition; i--)
                    ks = new PRow(window.order.items[i].Eval(cx, rb), ks);
                var dt = rb._rs.qry.rowType;
                for (var b = firstTie = fd.cur.wrs.PositionAt(cx, ks); b != null; b = b.Next(cx))
                {
                    for (var i = 0; i < dt.Length; i++)
                    {
                        var c = dt.columns[i];
                        var n = dt.columns[i].name;
                        if (rb.row[i] is TypedValue tv && c.domain.Compare(tv, b.row[i]) != 0)
                            goto skip;
                    }
                    fd.cur.wrb = b;
                    break;
                skip:;
                }
                for (var b = fd.cur.wrs.First(cx); b != null; b = b.Next(cx))
                    if (InWindow(cx, b))
                        switch (window.exclude)
                        {
                            case Sqlx.NO:
                                AddIn(cx, b);
                                break;
                            case Sqlx.CURRENT:
                                if (b._pos != fd.cur.wrb._pos)
                                    AddIn(cx, b);
                                break;
                            case Sqlx.TIES:
                                if (!Ties(fd.cur.wrb, b))
                                    AddIn(cx, b);
                                break;
                        }
                fd.valueInProgress = false;
            }
            TypedValue v = null;
            switch (kind)
            {
                case Sqlx.ABS:
                    v = val?.Eval(tr,cx)?.NotNull();
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
                case Sqlx.ANY: return TBool.For(fd.cur.bval);
                case Sqlx.ARRAY: // Mongo $push
                    {
                        if (window == null || fd.cur.mset == null || fd.cur.mset.Count == 0)
                            return fd.cur.acc;
                        fd.cur.acc = new TArray(new Domain(defpos,Sqlx.ARRAY, fd.cur.mset.tree?.First()?.key().dataType));
                        var ar = fd.cur.acc as TArray;
                        for (var d = fd.cur.mset.tree.First();d!= null;d=d.Next())
                            ar.list.Add(d.key());
                        return fd.cur.acc;
                    }
                case Sqlx.AVG:
                    {
                        switch (fd.cur.sumType.kind)
                        {
                            case Sqlx.NUMERIC: return new TReal(fd.cur.sumDecimal / new Common.Numeric(fd.cur.count));
                            case Sqlx.REAL: return new TReal(fd.cur.sum1 / fd.cur.count);
                            case Sqlx.INTEGER:
                                if (fd.cur.sumInteger != null)
                                    return new TReal(new Common.Numeric(fd.cur.sumInteger, 0) / new Common.Numeric(fd.cur.count));
                                return new TReal(new Common.Numeric(fd.cur.sumLong) / new Common.Numeric(fd.cur.count));
                        }
                        return domain.defaultValue;
                    }
                case Sqlx.CARDINALITY:
                    {
                        v = val?.Eval(tr,cx)?.NotNull();
                        if (v == null)
                            return null;
                        if (!(v.dataType.kind != Sqlx.MULTISET))
                            throw new DBException("42113", v).Mix();
                        var m = (TMultiset)v;
                        return new TInt(m.Count);
                    }
                case Sqlx.CASE:
                    {
                        v = val?.Eval(tr,cx)?.NotNull();
                        SqlFunction f = this;
                        for (; ; )
                        {
                            SqlFunction fg = f.op2 as SqlFunction;
                            if (fg == null)
                                return f.op2?.Eval(tr,cx)??null;
                            if (f.op1.domain.Compare(f.op1.Eval(tr,cx), v) == 0)
                                return f.val.Eval(tr,cx);
                            f = fg;
                        }
                    }
                case Sqlx.CAST:
                    {
                        v = val?.Eval(tr,cx)?.NotNull();
                        if (v == null)
                            return null;
                        return domain.Coerce(v);
                    }
                case Sqlx.CEIL: goto case Sqlx.CEILING;
                case Sqlx.CEILING:
                    v = val?.Eval(tr,cx)?.NotNull();
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
                        v = val?.Eval(tr,cx)?.NotNull();
                        if (v == null)
                            return domain.defaultValue;
                        if (v?.ToString().ToCharArray() is char[] chars)
                            return new TInt(chars.Length);
                        return new TInt(0);
                    }
                case Sqlx.CHARACTER_LENGTH: goto case Sqlx.CHAR_LENGTH;
             //   case Sqlx.CHECK: return new TRvv(rb);
                case Sqlx.COLLECT: return domain.Coerce((TypedValue)fd.cur.mset ??TNull.Value);
                //		case Sqlx.CONVERT: transcoding all seems to be implementation-defined TBD
                case Sqlx.COUNT: return new TInt(fd.cur.count);
                case Sqlx.CURRENT:
                    {
                        if (val.Eval(tr,cx) is TCursor tc && cx.values[tc.bmk._rs.qry.defpos] is TCursor tq)
                            return TBool.For(tc.bmk._pos == tq.bmk._pos);
                        break;
                    }
                case Sqlx.CURRENT_DATE: return new TDateTime(Domain.Date, DateTime.UtcNow);
                case Sqlx.CURRENT_ROLE: return new TChar(tr.role.name);
                case Sqlx.CURRENT_TIME: return new TDateTime(Domain.Timespan, DateTime.UtcNow);
                case Sqlx.CURRENT_TIMESTAMP: return new TDateTime(Domain.Timestamp, DateTime.UtcNow);
                case Sqlx.CURRENT_USER: return new TChar(tr.user.name);
                case Sqlx.ELEMENT:
                    {
                        v = val?.Eval(tr,cx)?.NotNull();
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
                    v = val?.Eval(tr,cx)?.NotNull();
                    if (v == null)
                        return null;
                    if (v == TNull.Value)
                        return domain.defaultValue;
                    return new TReal(Math.Exp(v.ToDouble()));
                case Sqlx.EVERY:
                    {
                        object o = fd.cur.mset.tree[TBool.False];
                        return (o == null || ((int)o) == 0) ? TBool.True : TBool.False;
                    }
                case Sqlx.EXTRACT:
                    {
                        v = val?.Eval(tr,cx)?.NotNull();
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
                case Sqlx.FIRST:  return fd.cur.mset.tree.First().key();
                case Sqlx.FLOOR:
                    v = val?.Eval(tr,cx)?.NotNull();
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
                case Sqlx.FUSION: return domain.Coerce(fd.cur.mset);
                case Sqlx.INTERSECTION:return domain.Coerce(fd.cur.mset);
                case Sqlx.LAST: return fd.cur.mset.tree.Last().key();
                case Sqlx.LN:
                    v = val?.Eval(tr,cx)?.NotNull();
                    if (v == null)
                        return null;
                    if (v.Val() == null)
                        return v;
                    return new TReal(Math.Log(v.ToDouble()));
                case Sqlx.LOCALTIME: return new TDateTime(Domain.Date, DateTime.Now);
                case Sqlx.LOCALTIMESTAMP: return new TDateTime(Domain.Timestamp, DateTime.Now);
                case Sqlx.LOWER:
                    {
                        v = val?.Eval(tr,cx)?.NotNull();
                        if (v == null)
                            return null;
                        string s = v.ToString();
                        if (s != null)
                            return new TChar(s.ToLower());
                        return domain.defaultValue;
                    }
                case Sqlx.MAX: return fd.cur.acc;
                case Sqlx.MIN: return fd.cur.acc;
                case Sqlx.MOD:
                    if (op1 != null)
                        v = op1.Eval(tr,cx);
                    if (v.Val() == null)
                        return domain.defaultValue;
                    switch (op1.domain.kind)
                    {
                        case Sqlx.INTEGER:
                            return new TInt(v.ToLong() % op2.Eval(tr,cx).ToLong());
                        case Sqlx.NUMERIC:
                            return new TNumeric(((Numeric)v.Val()) % (Numeric)op2.Eval(tr,cx).Val());
                    }
                    break;
                case Sqlx.NORMALIZE:
                    if (val != null)
                        v = val.Eval(tr,cx);
                    return v; //TBD
                case Sqlx.NULLIF:
                    {
                        TypedValue a = op1.Eval(tr,cx)?.NotNull();
                        if (a == null)
                            return null;
                        if (a.IsNull)
                            return domain.defaultValue;
                        TypedValue b = op2.Eval(tr,cx)?.NotNull();
                        if (b == null)
                            return null;
                        if (b.IsNull || op1.domain.Compare(a, b) != 0)
                            return a;
                        return domain.defaultValue;
                    }
                case Sqlx.OCTET_LENGTH:
                    {
                        v = val?.Eval(tr,cx)?.NotNull();
                        if (v == null)
                            return null;
                        if (v.Val() is byte[] bytes)
                            return new TInt(bytes.Length);
                        return domain.defaultValue;
                    }
                case Sqlx.OVERLAY:
                    v = val?.Eval(tr,cx)?.NotNull();
                    return v; //TBD
                case Sqlx.PARTITION:
                        return TNull.Value;
                case Sqlx.POSITION:
                    {
                        if (op1 != null && op2 != null)
                        {
                            string t = op1.Eval(tr,cx)?.ToString();
                            string s = op2.Eval(tr,cx)?.ToString();
                            if (t != null && s != null)
                                return new TInt(s.IndexOf(t));
                            return domain.defaultValue;
                        }
                        return TNull.Value;
                    }
                case Sqlx.POWER:
                    {
                        v = val?.Eval(tr,cx)?.NotNull();
                        if (v == null)
                            return null;
                        var w = op1?.Eval(tr,cx)?.NotNull();
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
                case Sqlx.ROW_NUMBER: return new TInt(fd.cur.wrb._pos+1);
                case Sqlx.SECURITY: // classification pseudocolumn
                    return TLevel.New(rb.Rec()?.classification ?? Level.D);
                case Sqlx.SET:
                    {
                        v = val?.Eval(tr,cx)?.NotNull();
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
                        if (fd.cur.count == 0)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, new TChar("StDev Pop"));
                        double m = fd.cur.sum1 / fd.cur.count;
                        return new TReal(Math.Sqrt((fd.cur.acc1 - 2 * fd.cur.count * m + fd.cur.count * m * m)
                            / fd.cur.count));
                    }
                case Sqlx.STDDEV_SAMP:
                    {
                        if (fd.cur.count <= 1)
                            throw new DBException("22004").ISO().Add(Sqlx.ROUTINE_NAME, new TChar("StDev Samp"));
                        double m = fd.cur.sum1 / fd.cur.count;
                        return new TReal(Math.Sqrt((fd.cur.acc1 - 2 * fd.cur.count * m + fd.cur.count * m * m)
                            / (fd.cur.count - 1)));
                    }
                case Sqlx.SUBSTRING:
                    {
                        v = val?.Eval(tr,cx)?.NotNull();
                        if (v == null)
                            return null;
                        string sv = v.ToString();
                        var w = op1?.Eval(tr,cx)??null;
                        if (sv == null || w == null)
                            return domain.defaultValue;
                        var x = op2?.Eval(tr,cx)??null;
                        if (x == null)
                            return new TChar((w == null || w.IsNull) ? null : sv.Substring(w.ToInt().Value));
                        return new TChar(sv.Substring(w.ToInt().Value, x.ToInt().Value));
                    }
                case Sqlx.SUM:
                    {
                        switch (fd.cur.sumType.kind)
                        {
                            case Sqlx.Null: return TNull.Value;
                            case Sqlx.NULL: return TNull.Value;
                            case Sqlx.REAL: return new TReal(fd.cur.sum1);
                            case Sqlx.INTEGER:
                                if (fd.cur.sumInteger != null)
                                    return new TInteger(fd.cur.sumInteger);
                                else
                                    return new TInt(fd.cur.sumLong);
                            case Sqlx.NUMERIC: return new TNumeric(fd.cur.sumDecimal);
                        }
                        throw new PEException("PE68");
                    }
                case Sqlx.TRANSLATE:
                     v = val?.Eval(tr,cx)?.NotNull();
                    return v; // TBD
                case Sqlx.TRIM:
                    {
                        v = val?.Eval(tr,cx)?.NotNull();
                        if (v == null)
                            return null;
                        if (v.IsNull)
                            return domain.defaultValue;
                        string sv = v.ToString();
                        object c = null;
                        if (op1 != null)
                        {
                            string s = op1.Eval(tr,cx).ToString();
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
                        v = val?.Eval(tr,cx)?.NotNull();
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
                        v = val?.Eval(tr,cx)?.NotNull();
                        if (v == null)
                            return null;
                        TypedValue a = op1.Eval(tr,cx);
                        if (a == TBool.True)
                            return v;
                        return op2.Eval(tr,cx);
                    }
                case Sqlx.XMLAGG: return new TChar(fd.cur.sb.ToString());
                case Sqlx.XMLCAST: goto case Sqlx.CAST;
                case Sqlx.XMLCOMMENT:
                    {
                        v = val?.Eval(tr,cx)?.NotNull();
                        if (v == null)
                            return null;
                        return new TChar("<!-- " + v.ToString().Replace("--", "- -") + " -->");
                    }
                //	case Sqlx.XMLCONCAT: break; see SqlValueExpr
                case Sqlx.XMLELEMENT:
                    {
                        object a = op2?.Eval(tr,cx)?.NotNull();
                        object x = val?.Eval(tr,cx)?.NotNull();
                        if (a == null || x == null)
                            return null;
                        string n = XmlConvert.EncodeName(op1.Eval(tr,cx).ToString());
                        string r = "<" + n  + " " + ((a == null) ? "" : XmlEnc(a)) + ">" +
                            ((x == null) ? "" : XmlEnc(x)) + "</" + n + ">";
                        //				trans.xmlns = "";
                        return new TChar(r);
                    }
                 case Sqlx.XMLPI:
                    v = val?.Eval(tr,cx)?.NotNull();
                    if (v == null)
                        return null;
                    return new TChar("<?" + v + " " + op1.Eval(tr,cx) + "?>");
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
                        v = val?.Eval(tr,cx)?.NotNull();
                        if (v == null)
                            return null;
                        return new TInt(Extract(tr, kind, v));
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
        long Extract(Transaction tr,Sqlx mod,TypedValue v)
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
        internal override SqlValue SetReg(Context _cx,TRow k)
        {
            var fd = _cx.func[defpos];
            if (fd == null)
                _cx.func += (defpos, fd = new FunctionData());
            if ((window!=null || aggregates0()))
            {
                fd.cur = fd.regs?[k];
                if (fd.cur == null)
                {
                    fd.cur = new FunctionData.Register() { profile = k };
                    if (fd.regs == null)
                        fd.regs = new CTree<TRow, FunctionData.Register>(k.dataType);
                    fd.regs+=(k, fd.cur);
                }
            } else
            {
                val.SetReg(_cx,k);
                op1.SetReg(_cx,k);
                op2.SetReg(_cx,k);
            }
            return this;
        } 
        /// <summary>
        /// for aggregates and window functions we need to implement StartCounter
        /// </summary>
        internal override void StartCounter(Context _cx,RowSet rs)
        {
            var fd = _cx.func[defpos];
            if (fd == null)
                _cx.func += (defpos, fd = new FunctionData());
            fd.cur.acc1 = 0.0;
            fd.cur.mset = null;
            switch (kind)
            {
                case Sqlx.ROW_NUMBER: break;
                case Sqlx.AVG:
                    fd.cur.count = 0L;
                    fd.cur.sumType = Domain.Content;
                    break;
                case Sqlx.COLLECT:
                case Sqlx.EVERY:
                case Sqlx.FUSION:
                case Sqlx.INTERSECTION:
                    fd.cur.mset = new TMultiset(val.domain);
                    break;
                case Sqlx.XMLAGG:
                    if (window != null)
                        goto case Sqlx.COLLECT;
                    fd.cur.sb = new StringBuilder();
                    break;
                case Sqlx.SOME:
                case Sqlx.ANY:
                    if (window != null)
                        goto case Sqlx.COLLECT;
                    fd.cur.bval = false;
                    break;
                case Sqlx.ARRAY:
                    fd.cur.acc = new TArray(new Domain(defpos,Sqlx.ARRAY, Domain.Content));
                    break;
                case Sqlx.COUNT:
                    fd.cur.count = 0L;
                    break;
                case Sqlx.FIRST:
                    fd.cur.acc = null; // NOT TNull.Value !
                    break;
                case Sqlx.LAST:
                    fd.cur.acc = TNull.Value;
                    break;
                case Sqlx.MAX:
                case Sqlx.MIN:
                    if (window != null)
                        goto case Sqlx.COLLECT;
                    fd.cur.sumType = Domain.Content;
                    fd.cur.acc = null;
                    break;
                case Sqlx.STDDEV_POP:
                    fd.cur.acc1 = 0.0;
                    fd.cur.sum1 = 0.0;
                    fd.cur.count = 0L;
                    break; 
                case Sqlx.STDDEV_SAMP: goto case Sqlx.STDDEV_POP;
                case Sqlx.SUM:
                    fd.cur.sumType = Domain.Content;
                    fd.cur.sumInteger = null;
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
        internal override void _AddIn(Context cx,RowBookmark rb,ref BTree<long,bool?>aggsDone)
        {
            if (aggsDone[defpos] == true)
                return;
            var tr = rb._rs._tr;
            var fd = cx.func[defpos];
            aggsDone +=(defpos, true);
            if (filter != null && !filter.Matches(tr,cx))
                return;
            if (mod == Sqlx.DISTINCT)
            {
                var v = val.Eval(cx,rb)?.NotNull();
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
                        var v = val.Eval(tr,cx);
                        if (v == null)
                        {
       //                     (tr as Transaction)?.Warning("01003");
                            break;
                        }
        //                etag = ETag.Add(v.etag, etag);
                    }
                    fd.cur.count++;
                    goto case Sqlx.SUM;
                case Sqlx.ANY:
                    {
                        if (window != null)
                            goto case Sqlx.COLLECT;
                        var v = val.Eval(tr,cx)?.NotNull();
                        if (v != null)
                        {
                            if (v.Val() is bool)
                                fd.cur.bval = fd.cur.bval || (bool)v.Val();
               //             else
               //                 (tr as Transaction)?.Warning("01003");
              //              etag = ETag.Add(v.etag, etag);
                        }
                        break;
                    }
                case Sqlx.ARRAY: // Mongo $push
                    if (val != null)
                    {
                        if (fd.cur.acc == null)
                            fd.cur.acc = new TArray(new Domain(defpos,Sqlx.ARRAY, val.domain));
                        var ar = fd.cur.acc as TArray;
                        var v = val.Eval(tr,cx)?.NotNull();
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
                            if (fd.cur.mset == null && val.Eval(tr,cx) != null)
                                fd.cur.mset = new TMultiset(val.domain);
                            var v = val.Eval(tr,cx)?.NotNull();
                            if (v != null)
                            {
                                fd.cur.mset.Add(v);
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
                            fd.cur.count++;
                            break;
                        }
                        var v = val.Eval(tr,cx)?.NotNull();
                        if (v != null)
                        {
                            fd.cur.count++;
            //                etag = ETag.Add(v.etag, etag);
                        }
                    }
                    break;
                case Sqlx.EVERY:
                    {
                        var v = val.Eval(tr,cx)?.NotNull();
                        if (v is TBool vb)
                        {
                            fd.cur.bval = fd.cur.bval && vb.value.Value;
   //                         etag = ETag.Add(v.etag, etag);
                        }
         //               else
         //                   tr.Warning("01003");
                        break;
                    }
                case Sqlx.FIRST:
                    if (val != null && fd.cur.acc == null)
                    {
                        fd.cur.acc = val.Eval(tr,cx)?.NotNull();
            //            if (fd.cur.acc != null)
             //           {
             //               domain = fd.cur.acc.dataType;
            //                etag = ETag.Add(cur.acc.etag, etag);
             //           }
                    }
                    break;
                case Sqlx.FUSION:
                    {
                        if (fd.cur.mset == null || fd.cur.mset.IsNull)
                        {
                            var vv = val.Eval(tr,cx)?.NotNull();
                            if (vv == null || vv.IsNull)
                                fd.cur.mset = new TMultiset(val.domain.elType); // check??
            //                else
            //                    (tr as Transaction)?.Warning("01003");
                        }
                        else
                        {
                            var v = val.Eval(tr,cx)?.NotNull();
                            fd.cur.mset = TMultiset.Union(fd.cur.mset, v as TMultiset, true);
              //              etag = ETag.Add(v?.etag, etag);
                        }
                        break;
                    }
                case Sqlx.INTERSECTION:
                    {
                        var v = val.Eval(tr,cx)?.NotNull();
               //         if (v == null)
               //             (tr as Transaction)?.Warning("01003");
               //         else
                        {
                            var mv = v as TMultiset;
                            if (fd.cur.mset == null || fd.cur.mset.IsNull)
                                fd.cur.mset = mv;
                            else
                                fd.cur.mset = TMultiset.Intersect(fd.cur.mset, mv, true);
               //             etag = ETag.Add(v.etag, etag);
                        }
                        break;
                    }
                case Sqlx.LAST:
                    if (val != null)
                    {
                        fd.cur.acc = val.Eval(tr,cx)?.NotNull();
                 //       if (fd.cur.acc != null)
                 //       {
                //            domain = cur.acc.dataType;
                //            etag = ETag.Add(val.etag, etag);
                 //       }
                    }
                    break;
                case Sqlx.MAX:
                    {
                        TypedValue v = val.Eval(tr,cx)?.NotNull();
                        if (v != null && (fd.cur.acc == null || fd.cur.acc.CompareTo(v) < 0))
                        {
                            fd.cur.acc = v;
               //             etag = ETag.Add(v.etag, etag);
                        }
              //          else
               //             (tr as Transaction)?.Warning("01003");
                        break;
                    }
                case Sqlx.MIN:
                    {
                        TypedValue v = val.Eval(tr,cx)?.NotNull();
                        if (v != null && (fd.cur.acc == null || fd.cur.acc.CompareTo(v) > 0))
                        {
                            fd.cur.acc = v;
             //             etag = ETag.Add(v.etag, etag);
                        }
             //           else
             //               (tr as Transaction)?.Warning("01003");
                        break;
                    }
                case Sqlx.STDDEV_POP: // not used for Remote
                    {
                        var o = val.Eval(tr,cx);
                        var v = o.ToDouble();
                        fd.cur.sum1 -= v;
                        fd.cur.acc1 -= v * v;
                        fd.cur.count--;
                        break;
                    }
                case Sqlx.STDDEV_SAMP: goto case Sqlx.STDDEV_POP;
                case Sqlx.SOME: goto case Sqlx.ANY;
                case Sqlx.SUM:
                    {
                        var v = val.Eval(tr,cx)?.NotNull();
                        if (v==null)
                        {
                //            tr.Warning("01003");
                            return;
                        }
               //         etag = ETag.Add(v.etag, etag);
                        switch (fd.cur.sumType.kind)
                        {
                            case Sqlx.CONTENT:
                                if (v is TInt)
                                {
                                    fd.cur.sumType = Domain.Int;
                                    fd.cur.sumLong = v.ToLong().Value;
                                    return;
                                }
                                if (v is TInteger)
                                {
                                    fd.cur.sumType = Domain.Int;
                                    fd.cur.sumInteger = (Integer)v.Val();
                                    return;
                                }
                                if (v is TReal)
                                {
                                    fd.cur.sumType = Domain.Real;
                                    fd.cur.sum1 = ((TReal)v).dvalue;
                                    return;
                                }
                                if (v is TNumeric)
                                {
                                    fd.cur.sumType = Domain.Numeric;
                                    fd.cur.sumDecimal = ((TNumeric)v).value;
                                    return;
                                }
                                break;
                            case Sqlx.INTEGER:
                                if (v is TInt)
                                {
                                    long a = v.ToLong().Value;
                                    if (fd.cur.sumInteger == null)
                                    {
                                        if ((a > 0) ? (fd.cur.sumLong <= long.MaxValue - a) : (fd.cur.sumLong >= long.MinValue - a))
                                            fd.cur.sumLong += a;
                                        else
                                            fd.cur.sumInteger = new Integer(fd.cur.sumLong) + new Integer(a);
                                    }
                                    else
                                        fd.cur.sumInteger = fd.cur.sumInteger + new Integer(a);
                                    return;
                                }
                                if (v is TInteger)
                                {
                                    Integer a = ((TInteger)v).ivalue;
                                    if (fd.cur.sumInteger == null)
                                        fd.cur.sumInteger = new Integer(fd.cur.sumLong) + a;
                                    else
                                        fd.cur.sumInteger = fd.cur.sumInteger + a;
                                    return;
                                }
                                break;
                            case Sqlx.REAL:
                                if (v is TReal)
                                {
                                    fd.cur.sum1 += ((TReal)v).dvalue;
                                    return;
                                }
                                break;
                            case Sqlx.NUMERIC:
                                if (v is TNumeric)
                                {
                                    fd.cur.sumDecimal = fd.cur.sumDecimal + ((TNumeric)v).value;
                                    return;
                                }
                                break;
                        }
                        throw new DBException("22108").Mix();
                    }
                case Sqlx.XMLAGG:
                    {
                        fd.cur.sb.Append(' ');
                        var o = val.Eval(tr,cx)?.NotNull();
                        if (o != null)
                        {
                            fd.cur.sb.Append(o.ToString());
                 //           etag = ETag.Add(o.etag, etag);
                        }
                //        else
                 //           tr.Warning("01003");
                        break;
                    }
                default:
                    val?.AddIn(cx,rb);
                    break;
            }
        }
        /// <summary>
        /// Window Functions: bmk is a bookmark in cur.wrs
        /// </summary>
        /// <param name="bmk"></param>
        /// <returns></returns>
        bool InWindow(Context _cx, RowBookmark bmk)
        {
            if (bmk == null)
                return false;
            var tr = bmk._rs._tr;
            if (window.units == Sqlx.RANGE && !(TestStartRange(_cx,bmk) && TestEndRange(_cx,bmk)))
                return false;
            if (window.units == Sqlx.ROWS && !(TestStartRows(_cx,bmk) && TestEndRows(_cx,bmk)))
                return false;
            return true;
        }
        /// <summary>
        /// Test the window against the end of the given rows measured from cur.wrb
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestEndRows(Context _cx, RowBookmark bmk)
        {
            if (window.high == null || window.high.unbounded)
                return true;
            var fd = _cx.func[defpos];
            long limit;
            if (window.high.current)
                limit = fd.cur.wrb._pos;
            else if (window.high.preceding)
                limit = fd.cur.wrb._pos - (window.high.distance?.ToLong()??0);
            else
                limit = fd.cur.wrb._pos + (window.high.distance?.ToLong()??0);
            return bmk._pos <= limit; 
        }
        /// <summary>
        /// Test a window against the start of a rows
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestStartRows(Context _cx, RowBookmark bmk)
        {
            if (window.low == null || window.low.unbounded)
                return true;
            var fd = _cx.func[defpos];
            long limit;
            if (window.low.current)
                limit =fd.cur.wrb._pos;
            else if (window.low.preceding)
                limit = fd.cur.wrb._pos - (window.low.distance?.ToLong() ?? 0);
            else
                limit = fd.cur.wrb._pos + (window.low.distance?.ToLong() ?? 0);
            return bmk._pos >= limit;
        }

        /// <summary>
        /// Test the window against the end of the given range
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestEndRange(Context _cx, RowBookmark bmk)
        {
            if (window.high == null || window.high.unbounded)
                return true;
            var fd = _cx.func[defpos];
            var n = val.defpos;
            var kt = val.domain;
            var wrv = fd.cur.wrb.row[n];
            TypedValue limit;
            var tr = bmk._rs._tr;
            if (window.high.current)
                limit = wrv;
            else if (window.high.preceding)
                limit = kt.Eval(tr,defpos,wrv, (kt.AscDesc == Sqlx.ASC) ? Sqlx.MINUS : Sqlx.PLUS, window.high.distance);
            else
                limit = kt.Eval(tr,defpos,wrv, (kt.AscDesc == Sqlx.ASC) ? Sqlx.PLUS : Sqlx.MINUS, window.high.distance);
            return kt.Compare(bmk.row[n], limit) <= 0; 
        }
        /// <summary>
        /// Test a window against the start of a range
        /// </summary>
        /// <returns>whether the window is in the range</returns>
        bool TestStartRange(Context _cx, RowBookmark bmk)
        {
            if (window.low == null || window.low.unbounded)
                return true;
            var fd = _cx.func[defpos];
            var n = val.defpos;
            var kt = val.domain;
            var tv = fd.cur.wrb.row[n];
            TypedValue limit;
            var tr = bmk._rs._tr;
            if (window.low.current)
                limit = tv;
            else if (window.low.preceding)
                limit = kt.Eval(tr,defpos,tv, (kt.AscDesc != Sqlx.DESC) ? Sqlx.PLUS : Sqlx.MINUS, window.low.distance);
            else
                limit = kt.Eval(tr,defpos,tv, (kt.AscDesc != Sqlx.DESC) ? Sqlx.MINUS : Sqlx.PLUS, window.low.distance);
            return kt.Compare(bmk.row[n], limit) >= 0; // OrderedKey comparison
        }
        /// <summary>
        /// Alas, we can't use Value() here, as the value is still in progress.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns>whether b ties a</returns>
        bool Ties(RowBookmark a,RowBookmark b)
        {
            for (var i = window.partition; i < window.order.items.Count; i++)
            {
                var oi = window.order.items[i];
                var n = oi.defpos;
                var av = a.row[n];
                var bv = b.row[n];
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
        internal override SqlValue _ColsForRestView(long dp, From gf, GroupSpecification gs, 
            ref BTree<SqlValue, string> gfc, ref BTree<SqlValue, string> rem, 
            ref BTree<string, bool?> reg,ref BTree<long,SqlValue>map)
        {
            var ac = "C_" + defpos;
            var qs = gf.QuerySpec();
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
                        var s0 = new SqlValue(dp, n0, BTree<long,object>.Empty
                            +(_Domain,domain)+(Query,gf));
                        var s1 = new SqlValue(dp, n1, BTree<long, object>.Empty
                            + (_Domain, Domain.Int) + (Query, gf));
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
                        st = new SqlValue(dp, an, BTree<long, object>.Empty
                            + (_Domain, Domain.Int) + (Query, gf));
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
                        var s0 = new SqlValue(dp, n0, BTree<long, object>.Empty
                            + (_Domain, domain) + (Query, gf));
                        var s1 = new SqlValue(dp, n1, BTree<long, object>.Empty
                            + (_Domain, domain) + (Query, gf));
                        var s2 = new SqlValue(dp, n2, BTree<long, object>.Empty
                            + (_Domain, domain) + (Query, gf));
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
                            var va = new SqlValue(dp, vn, BTree<long,object>.Empty
                                +(_Domain,vt)+(Query, gf));
                            var sf = new SqlFunction(dp, nk, 
                                va, op1, op2, mod,new BTree<long,object>(_Alias,an));
                            gfc+=(va, vn);
                            map+=(defpos, sf);
                            return sf;
                        }
                        if (aggregates())
                        {
                            var sr = new SqlFunction(dp, kind,
                                val._ColsForRestView(dp, gf, gs, ref gfc, ref rem, ref reg, ref map),
                                op1, op2, mod, new BTree<long, object>(_Alias, an));
                            map+=(defpos, sr);
                            return sr;
                        }
                        var r = this+(_Alias,an);
                        gfc+=(r, an);
                        rem+=(r, an);
                        var sn = new SqlValue(dp,an, BTree<long,object>.Empty
                            +(Query,gf)+(_Domain,domain));
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
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            return (op1.Eval(tr,cx) is TypedValue lf) ? 
                ((lf == TNull.Value) ? op2.Eval(tr,cx) : lf) : null;
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
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            TypedValue v = null;
            if (op1 != null)
                v = op1.Eval(tr,cx);
            if (v==null || v.IsNull)
                return domain.defaultValue;
            var st = v.dataType;
            if (st.iri != null)
                return v;
 /*           var d = LocalTrans(op1);
            if (d == null)
                return domain.defaultValue;
            long td = st.DomainDefPos(d,-1);
            if (td > 0)
            {
                var dt = d.GetDataType(td);
                if (dt != null)
                    return new TChar( dt.iri);
            } */
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
        internal override DBObject Relocate(long dp)
        {
            return new QuantifiedPredicate(dp,mem);
        }
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var w = (SqlValue)what.Relocate(wr);
            if (w != what)
                r += (What, w);
            var s = (QuerySpecification)select.Relocate(wr);
            if (s != select)
                r += (Select, s);
            return r;
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (QuantifiedPredicate)base.Replace(cx, so, sv);
            var wh = (SqlValue)r.what.Replace(cx,so,sv);
            if (wh != r.what)
                r += (What, wh);
            var se = r.select.Replace(cx, so, sv);
            if (se != r.select)
                r += (Select, se);
            cx.done += (defpos, r);
            return r;
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
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            for (var rb = select.RowSets(tr,cx).First(cx); rb != null; rb = rb.Next(cx))
            {
                var col = rb.row[0];
                if (what.Eval(tr,cx) is TypedValue w)
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
        internal override void _AddIn(Context _cx,RowBookmark rb, ref BTree<long, bool?> aggsDone)
        {
            what._AddIn(_cx,rb, ref aggsDone);
            select.AddIn(_cx,rb);
        }
        internal override SqlValue SetReg(Context _cx,TRow k)
        {
            select.SetReg(_cx,k);
            return (what.SetReg(_cx,k)!=null)?this:null;
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
            var r = base.Relocate(wr);
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (BetweenPredicate)base.Replace(cx, so, sv);
            var wh = (SqlValue)r.what.Replace(cx,so,sv);
            if (wh != r.what)
                r += (QuantifiedPredicate.What, wh);
            var lw = (SqlValue)r.low.Replace(cx,so,sv);
            if (lw != r.low)
                r += (QuantifiedPredicate.Low, lw);
            var hg = (SqlValue)r.high.Replace(cx,so,sv);
            if (hg != r.high)
                r += (QuantifiedPredicate.High, hg);
            cx.done += (defpos, r);
            return r;
        }
        internal override bool Uses(long t)
        {
            return low.Uses(t) || high.Uses(t) || what.Uses(t);
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
        internal override void _AddIn(Context _cx,RowBookmark rb, ref BTree<long, bool?> aggsDone)
        {
            what._AddIn(_cx,rb, ref aggsDone);
            low._AddIn(_cx,rb, ref aggsDone);
            high._AddIn(_cx,rb, ref aggsDone);
        }
        internal override void OnRow(RowBookmark bmk)
        {
            what.OnRow(bmk);
            low.OnRow(bmk);
            high.OnRow(bmk);
        }
        internal override SqlValue SetReg(Context _cx,TRow k)
        {
            var a = what.SetReg(_cx,k) != null;
            var b = low.SetReg(_cx,k) != null;
            var c = high.SetReg(_cx,k) != null;
            return (a || b || c) ? this : null; // avoid shortcut evaluation
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
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            if (what.Eval(tr,cx) is TypedValue w)
            {
                var t = what.domain;
                if (low.Eval(tr,cx) is TypedValue lw)
                {
                    if (t.Compare(w, t.Coerce(lw)) < 0)
                        return TBool.False;
                    if (high.Eval(tr,cx) is TypedValue hg)
                        return TBool.For(t.Compare(w, t.Coerce(hg)) <= 0);
                }
            }
            return null;
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (LikePredicate)base.Replace(cx, so, sv);
            var wh = r.left.Replace(cx,so,sv);
            if (wh != r.left)
                r += (Left, wh);
            var rg = r.right.Replace(cx, so, sv);
            if (rg != r.right)
                r += (Right, rg);
            var esc = r.escape.Replace(cx, so, sv);
            if (esc != r.escape)
                r += (Escape, esc);
            cx.done += (defpos, r);
            return r;
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
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            bool r = false;
            if (left.Eval(tr,cx)?.NotNull() is TypedValue lf && right.Eval(tr,cx)?.NotNull() is TypedValue rg)
            {
                if (lf.IsNull && rg.IsNull)
                    r = true;
                else if ((!lf.IsNull) & !rg.IsNull)
                {
                    string a = lf.ToString();
                    string b = rg.ToString();
                    string e = "\\";
                    if (escape != null)
                        e = escape.Eval(tr,cx).ToString();
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
        internal override void _AddIn(Context _cx,RowBookmark rb, ref BTree<long, bool?> aggsDone)
        {
            left._AddIn(_cx,rb, ref aggsDone);
            right._AddIn(_cx,rb, ref aggsDone);
        }
        internal override SqlValue SetReg(Context _cx,TRow k)
        {
            return (left.SetReg(_cx,k) != null || right.SetReg(_cx,k) != null) ? this : null;
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
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var w = (SqlValue)what.Relocate(wr);
            if (w != what)
                r += (QuantifiedPredicate.What, w);
            var wh = (QuerySpecification)where.Relocate(wr);
            if (wh != where)
                r += (QuantifiedPredicate.Where, wh);
            return r;
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (InPredicate)base.Replace(cx, so, sv);
            var wh = r.what.Replace(cx,so,sv);
            if (wh != r.what)
                r += (QuantifiedPredicate.What, wh);
            var wr = r.where.Replace(cx, so, sv);
            if (wr != r.where)
                r += (QuantifiedPredicate.Select, wr);
            var vs = r.vals;
            for (var b=vs.First();b!=null;b=b.Next())
            {
                var v = b.value().Replace(cx,so,sv);
                if (v != b.value())
                    vs += (b.key(), (SqlValue)v);
            }
            if (vs != r.vals)
                r += (QuantifiedPredicate.Vals, vs);
            cx.done += (defpos, r);
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
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            if (what.Eval(tr,cx) is TypedValue w)
            {
                if (vals != BList<SqlValue>.Empty)
                {
                    for (var v = vals.First();v!=null;v=v.Next())
                        if (v.value().domain.Compare(w, v.value().Eval(tr,cx)) == 0)
                            return TBool.For(found);
                    return TBool.For(!found);
                }
                else
                {
                    for (var rb = where.RowSets(tr,cx).First(cx); rb != null; rb = rb.Next(cx))
                    {
                        if (w.dataType.kind!=Sqlx.ROW)
                        {
                            var v = rb.row.columns[0];
                            if (w.CompareTo(v) == 0)
                                return TBool.For(found);
                        }
                        else if (w.CompareTo(rb.row) == 0)
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
        internal override void _AddIn(Context _cx,RowBookmark rb, ref BTree<long, bool?> aggsDone)
        {
            for (var v = vals.First(); v != null; v = v.Next())
                v.value()._AddIn(_cx,rb, ref aggsDone);
            what._AddIn(_cx,rb, ref aggsDone);
            base._AddIn(_cx,rb, ref aggsDone);
        }
        internal override SqlValue SetReg(Context _cx,TRow k)
        {
            bool nulls = true;
            for (var v = vals.First(); v != null; v = v.Next())
                if (v.value().SetReg(_cx,k) != null)
                    nulls = false;
            return (what.SetReg(_cx,k) == null && nulls) ? null : this;
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
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var lh = (SqlValue)lhs.Relocate(wr);
            if (lh != lhs)
                r += (Lhs, lh);
            var rh = (SqlValue)rhs.Relocate(wr);
            if (rh != rhs)
                r += (Rhs, rh);
            return r;
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (MemberPredicate)base.Replace(cx, so, sv);
            var lf = r.left.Replace(cx,so,sv);
            if (lf != r.left)
                r += (Lhs,lf);
            var rg = r.rhs.Replace(cx,so,sv);
            if (rg != r.rhs)
                r += (Rhs,rg);
            cx.done += (defpos, r);
            return r;
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
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            if (lhs.Eval(tr,cx) is TypedValue a && rhs.Eval(tr,cx) is TypedValue b)
            {
                if (b.IsNull)
                    return domain.defaultValue;
                if (a.IsNull)
                    return TBool.False;
                if (b is TMultiset m)
                    return m.tree.Contains(a) ? TBool.True : TBool.False;
                throw tr.Exception("42113", b.GetType().Name).Mix();
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
        internal override void _AddIn(Context _cx,RowBookmark rb, ref BTree<long, bool?> aggsDone)
        {
            lhs._AddIn(_cx,rb, ref aggsDone);
            rhs._AddIn(_cx,rb, ref aggsDone);
        }
        internal override SqlValue SetReg(Context _cx,TRow k)
        {
            return (lhs.SetReg(_cx,k) != null || rhs.SetReg(_cx,k) != null) ? this : null;
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (TypePredicate)base.Replace(cx, so, sv);
            var lh = r.lhs.Replace(cx, so, sv);
            if (lh != r.lhs)
                r += (MemberPredicate.Lhs, lh);
            cx.done += (defpos, r);
            return r;
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
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            var a = lhs.Eval(tr,cx);
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
        internal override TypedValue Eval(Context _cx, RowBookmark bmk)
        {
            var rs = bmk._rs;
            var tr = rs._tr;
            var a = lhs.Eval(_cx,bmk);
            if (a == null)
                return null;
            if (a.IsNull)
                return TBool.False;
            bool b = false;
            var at = a.dataType;
            for (var t = rhs.First();t!=null;t=t.Next())
                b = at.EqualOrStrongSubtypeOf(t.value()); // implemented as Equals for ONLY
            return TBool.For(b == found);
        }
    }
    /// <summary>
    /// SQL2011 defined some new predicates for period
    /// </summary>
    internal class PeriodPredicate : SqlValue
    {
        public Sqlx op => (Sqlx)(mem[SqlValueExpr.Kind]??Sqlx.NO);
        public PeriodPredicate(long dp,SqlValue op1, Sqlx o, SqlValue op2) 
            :base(dp,BTree<long,object>.Empty+(_Domain,Domain.Bool)
                 +(Left,op1)+(Right,op2)+(SqlValueExpr.Kind,op1)
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (PeriodPredicate)base.Replace(cx, so, sv);
            var a = r.left.Replace(cx,so,sv);
            if (a != r.left)
                r += (Left,a);
            var b = r.right.Replace(cx,so,sv);
            if (b != r.right)
                r += (Right,b);
            cx.done += (defpos, r);
            return r;
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
        internal override void _AddIn(Context _cx,RowBookmark rb, ref BTree<long, bool?> aggsDone)
        {
            left?._AddIn(_cx,rb, ref aggsDone);
            right?._AddIn(_cx,rb, ref aggsDone);
        }
        internal override SqlValue SetReg(Context _cx,TRow k)
        {
            return (left?.SetReg(_cx,k) != null || right?.SetReg(_cx,k) != null) ? this : null;
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
            var r = base.Relocate(wr);
            var q = (Query)expr.Relocate(wr);
            if (q != expr)
                r += (QExpr, q);
            return r;
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
            var cols = expr.rowType.columns;
            for (var i = 0; i < cols.Count; i++)
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
        internal override void _AddIn(Context _cx,RowBookmark rb, ref BTree<long, bool?> aggsDone)
        {
            expr.AddIn(_cx,rb);
            base._AddIn(_cx,rb, ref aggsDone);
        }
        internal override SqlValue SetReg(Context _cx,TRow k)
        {
            expr.SetReg(_cx,k);
            return base.SetReg(_cx,k);
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (ExistsPredicate)base.Replace(cx, so, sv);
            var ex = r.expr.Replace(cx, so, sv);
            if (ex != r.expr)
                r += (QExpr, ex);
            cx.done += (defpos, r);
            return r;
        }
        /// <summary>
        /// The predicate is true if the rowSet has at least one element
        /// </summary>
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            return TBool.For(expr.RowSets(tr,cx).First(cx)!=null);
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (UniquePredicate)base.Replace(cx, so, sv);
            var ex = r.expr.Replace(cx, so, sv);
            if (ex != r.expr)
                r += (QExpr, ex);
            cx.done += (defpos, r);
            return r;
        }
        /// <summary>
        /// the predicate is true if the rows are distinct 
        /// </summary>
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            RowSet rs = expr.RowSets(tr,cx);
            RTree a = new RTree(rs,new TreeInfo(rs.rowType, TreeBehaviour.Disallow, TreeBehaviour.Disallow));
            cx.obs +=(expr.defpos, expr);
            for (var rb=rs.First(cx);rb!= null;rb=rb.Next(cx))
                if (RTree.Add(ref a, rb.row, rb.row) == TreeBehaviour.Disallow)
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
        internal override Basis Relocate(Writer wr)
        {
            var r = base.Relocate(wr);
            var nv = (SqlValue)val.Relocate(wr);
            if (nv != val)
                r += (NVal, nv);
            return r;
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (NullPredicate)base.Replace(cx, so, sv);
            var vl = r.val.Replace(cx,so,sv);
            if (vl != r.val)
                r += (NVal, vl);
            cx.done += (defpos, r);
            return r;
        }
        /// <summary>
        /// Test to see if the value is null in the current row
        /// </summary>
        internal override TypedValue Eval(Transaction tr, Context cx)
        {
            return (val.Eval(tr,cx) is TypedValue tv)? TBool.For(tv.IsNull == isnull) : null;
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
        internal override void _AddIn(Context _cx,RowBookmark rb, ref BTree<long, bool?> aggsDone)
        {
            val._AddIn(_cx,rb, ref aggsDone);
        }
        internal override SqlValue SetReg(Context _cx,TRow k)
        {
            return (val.SetReg(_cx,k) != null) ? this : null;
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlHttpBase)base.Replace(cx,so,sv);
            var gf = r.globalFrom.Replace(cx, so, sv);
            if (gf != r.globalFrom)
                r += (GlobalFrom, gf);
            var wh = r.where;
            for (var b=wh.First();b!=null;b=b.Next())
            {
                var v = b.value().Replace(cx,so,sv);
                if (v != b.value())
                    wh += (b.key(), (SqlValue)v);
            }
            if (wh != r.where)
                r += (HttpWhere, wh);
            var ma = r.matches;
            for (var b=ma.First();b!=null;b=b.Next())
            {
                var v = b.key().Replace(cx, so, sv);
                if (v != b.key())
                    ma += ((SqlValue)v, b.value());
            }
            if (ma != r.matches)
                r += (HttpMatches, ma);
            cx.done += (defpos, r);
            return r;
        }
        internal virtual SqlHttpBase AddCondition(Context cx,SqlValue wh)
        {
            return (wh!=null)? this+(HttpWhere,where+(wh.defpos, wh)):this;
        }
        internal void AddCondition(Context cx,BTree<long,SqlValue> svs)
        {
            for (var b = svs.First(); b != null; b = b.Next())
                AddCondition(cx,b.value());
        }
        internal abstract TypedValue Eval(Transaction tr, Context cx, bool asArray);
        internal override TypedValue Eval(Transaction tr, Context cx)
        {
            return Eval(tr, cx,false);
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
        internal SqlHttp(long dp, Query gf, SqlValue v, string m, ObInfo tgt,
            BTree<long, SqlValue> w, string rCols, TRow ur = null, BTree<SqlValue, TypedValue> mts = null)
            : base(dp,gf,BTree<long,object>.Empty+(HttpWhere,w)+(HttpMatches,mts)
                  +(Url,v)+(Mime,m)+(Pre,ur)+(TargetType,tgt)+(RemoteCols,rCols))
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlHttp)base.Replace(cx,so,sv);
            var u = r.expr.Replace(cx,so,sv);
            if (u != r.expr)
                r += (Url, u);
            cx.done += (defpos, r);
            return r;
        }
        /// <summary>
        /// A lot of the fiddly rowType calculation is repeated from RestView.RowSets()
        /// - beware of these mutual dependencies
        /// </summary>
        /// <param name="cx"></param>
        /// <returns></returns>
        internal override TypedValue Eval(Transaction tr,Context cx,bool asArray)
        {
            var r = (tr!=null && expr?.Eval(tr,cx) is TypedValue ev)?
                    OnEval(cx,ev):null;
            if ((!asArray) && r is TArray ta)
                return new TRowSet(tr,cx,globalFrom, globalFrom.rowType,ta.ToArray());
            if (r != null)
                return r;
            return TNull.Value;
        }
        TypedValue OnEval(Context cx, TypedValue ev)
        {
            string url = ev.ToString();
            var rx = url.LastIndexOf("/");
            var rtype = globalFrom.source.rowType;
            var vw = globalFrom.target as View;
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
                var qs = globalFrom.QuerySpec();
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
                    for (var b = globalFrom.rowType.columns.First(); b != null; b = b.Next())
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
                if (gf.rowType.map.Contains(g.name) && !ids.Contains(g.name))
                {
                    ids.Add(g.name);
                    sql.Append(cm); cm = ",";
                    sql.Append(g.name);
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
            var r = base.Relocate(wr);
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
        internal override SqlHttpBase AddCondition(Context cx,SqlValue wh)
        {
            return base.AddCondition(cx,wh?.PartsIn(globalFrom.source.rowType));
        }
        internal override TypedValue Eval(Transaction tr, Context cx, bool asArray)
        {
            var qs = globalFrom.QuerySpec(); // can be a From if we are in a join
            var cs = globalFrom.source as CursorSpecification;
            cs.MoveConditions(cx,cs.usingFrom); // probably updates all the queries
            qs = globalFrom.QuerySpec();
            cs = globalFrom.source as CursorSpecification;
            var uf = cs.usingFrom;
            var usingTable = uf.target as Table;
            var usingIndex = usingTable.FindPrimaryIndex(tr);
            var ut = tr.role.obinfos[usingTable.defpos] as ObInfo;
            var usingTableColumns = ut.columns;
            var urs = new IndexRowSet(tr, cx, uf, usingIndex, null) { matches = uf.matches };
            var ers = new ExplicitRowSet(tr, cx, cs);
            for (var b = urs.First(cx); b != null; b = b.Next(cx))
            {
                var ur = b.row;
                var url = ur[usingTableColumns[(int)usingTableColumns.Count - 1].defpos];
                var sv = new SqlHttp(defpos, globalFrom, new SqlLiteral(-1,url), "application/json", 
                    globalFrom.source.rowType,
                    globalFrom.where, cs.ToString(), ur, globalFrom.matches);
                if (sv.Eval(tr, cx, true) is TArray rv)
                    for (var i = 0; i < rv.Length; i++)
                        ers.Add((-1,rv[i] as TRow));
                else
                    return null;
            }
            return new TRowSet(ers);
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
