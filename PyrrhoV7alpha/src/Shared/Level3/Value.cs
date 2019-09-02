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
            _Alias = -315, // string
            _From = -316, // Query
            IsSetup = -317, // bool
            Left = -318, // SqlValue
            NominalType = -319, // Domain
            Right = -320, // SqlValue
            Sub = -321, // SqlValue
            TableRow = -322; // TableRow 
        internal Domain nominalDataType => (Domain)mem[NominalType]??Domain.Content;
        internal SqlValue left => (SqlValue)mem[Left];
        internal SqlValue right => (SqlValue)mem[Right];
        internal SqlValue sub => (SqlValue)mem[Sub];
        internal string alias => (string)mem[_Alias];
        internal bool isSetup => (bool)(mem[IsSetup]??false);
        internal Query from => (Query)mem[_From];
        protected SqlValue(long dp,BTree<long,object> m):base(dp,m)
        { }
        public static SqlValue operator+(SqlValue s,(long,object)x)
        {
            return (SqlValue)s.New(s.mem + x);
        }
        /// <summary>
        /// We are assisting Query.Changed. If we haven't got q we will return null,
        /// otherwise we will return an updated version of this.
        /// </summary>
        /// <param name="q"></param>
        /// <returns></returns>
        internal virtual SqlValue Changed(Query q)
        {
            return null;
        }
        internal virtual SqlValue Queries(long dp)
        {
            return null;
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
        /// But if a selector in the view is COUNT(*) this will not
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
        internal virtual void Disjunction(List<SqlValue> cond)
        {
            cond.Add(this);
        }
        internal virtual Query Resolve(Context cx,Query f)
        {
            return f;
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
        internal virtual int ColFor(Query q)
        {
            return q.scols[this] ?? -1;
        }
        internal virtual TypedValue Eval(Context _cx, RowBookmark bmk)
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
        internal virtual Context Ctx()
        {
            return null;
        }
        internal virtual SqlValue Find(long defpos, Ident n)
        {
            if (n == null)
                return this;
            var iq = nominalDataType.Get(ref n,out Ident sub);
            if (iq.HasValue)
            {
                var fn = nominalDataType.columns[iq.Value];
                return new SqlField(defpos,nominalDataType.columns[iq.Value].domain, this, n).Find(defpos,sub);
            }
            return null;
        }

        internal void Expected(ref Domain t, Domain exp)
        {
            var nt = t.Constrain(exp) ?? throw new DBException("42161", exp, t).Mix();
            t = nt;
        }
        internal SqlValue Constrain(Domain dt)
        {
            return new SqlValue(defpos,mem+(NominalType, nominalDataType.Constrain(dt)));
        }
        internal virtual void Build(Context _cx,RowSet rs)
        {
        }
        /// <summary>
        /// If the value contains aggregates we need to accumulate them
        /// </summary>
        internal virtual void StartCounter(Context _cx,RowSet rs)
        {
        }
        internal virtual void AddIn(Context _cx,RowBookmark rb)
        {
        }
        /// <summary>
        /// If the value contains aggregates we need to accumulate them. 
        /// Carefully watch out for common subexpressions, and only AddIn once!
        /// </summary>
        internal virtual void _AddIn(Context _cx,RowBookmark rb, ref BTree<long, bool?> aggsDone) { }

        /// <summary>
        /// Used for Window Function evaluation.
        /// Called by GroupingBookmark (when not building) and SelectedRowBookmark
        /// </summary>
        /// <param name="bmk"></param>
        internal virtual void OnRow(RowBookmark bmk)
        { }
        /// <summary>
        /// If the value is in a grouped construct then every column reference needs to be aggregated or grouped
        /// </summary>
        /// <param name="group">the group by clause</param>
        internal virtual bool Check(Transaction tr, GroupSpecification group)
        {
            if (group == null)
                return true;
            if (group.Has(this))
                return false;
            return false;
        }
        /// <summary>
        /// analysis stage Sources() and Selects(): setup SqlValue operands
        /// </summary>
        /// <param name="d">The required data type or null</param>
        internal virtual SqlValue _Setup(Transaction tr,Context cx,Query q,Domain d)
        {
            return (nominalDataType==Domain.Null)?this+(NominalType,d):this;
        }
        internal static SqlValue Setup(Transaction tr,Context cx,Query q,SqlValue v,Domain d)
        {
            if (v == null)
                return null;
            if (v is Selector sn)
                throw new PEException("PE425");
            d = d.LimitBy(v.nominalDataType);
            return (SqlValue)v.New(v._Setup(tr, cx, q, d).mem+(IsSetup,true));
        }
        internal static void Setup(Transaction tr, Context cx, Query q, BTree<long,SqlValue>t, Domain d)
        {
            for (var b = t.First(); b != null; b = b.Next())
                Setup(tr, cx, q, b.value(), d);
        }
        /// <summary>
        /// analysis stage Conditions(). See if q can fully evaluate this.
        /// If so, evaluation of an enclosing QuerySpec column can be moved down to q.
        /// </summary>
        internal virtual Query Conditions(Transaction tr,Context cx,Query q,bool disj,out bool move)
        {
            move = false;
            return q;
        }
        internal virtual SqlValue Simplify(Transaction tr,Query source)
        {
            return this;
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
            return (defpos == so.defpos) ? sv : this;
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
        internal virtual Query JoinCondition(Context cx, JoinPart j, ref BTree<long, SqlValue> joinCond, ref BTree<long, SqlValue> where)
        {
            j += (JoinPart.LeftOperand, j.left + (Query.Where, j.left.where + (defpos, this)));
            j += (JoinPart.RightOperand, j.right + (Query.Where, j.right.where + (defpos, this)));
            return j;
        }
        /// <summary>
        /// Analysis stage Conditions: Distribute conditions to joins, froms
        /// </summary>
        /// <param name="q"> Query</param>
        /// <param name="repl">Updated list of equality conditions for possible replacements</param>
        /// <param name="needed">Updated list of fields mentioned in conditions</param>
        internal virtual Query DistributeConditions(Transaction tr,Context cx,Query q, BTree<SqlValue,SqlValue> repl)
        {
            return q;
        }
        internal virtual SqlValue PartsIn(Domain dt)
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
        internal virtual int? ColFor(RowSet rs)
        {
            return rs.qry.scols[this];
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
        public virtual int CompareTo(object obj)
        {
            return (obj is SqlValue that)?defpos.CompareTo(that.defpos):1;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(_Alias)) { sb.Append(" Alias="); sb.Append(alias); }
            if (mem.Contains(Left)) { sb.Append(" Left:"); sb.Append(left); }
            if (mem.Contains(NominalType)) { sb.Append(" NominalType"); sb.Append(nominalDataType); }
            if (mem.Contains(Right)) { sb.Append(" Right:"); sb.Append(right); }
            if (mem.Contains(Sub)) { sb.Append(" Sub:"); sb.Append(sub); }
            return sb.ToString();
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
        internal virtual SqlValue _ColsForRestView(long dp, Query gf, GroupSpecification gs,  
            ref BTree<SqlValue,string> gfc, ref BTree<SqlValue,string> rem, ref BTree<string,bool?> reg,
            ref BTree<long,SqlValue> map)
        {
            var an = "A" + defpos;
            gfc = new BTree<SqlValue,string>(this,an);
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
        internal SqlValue ColsForRestView(Transaction tr, long dp, Context cx, Query gf,GroupSpecification gs, ref BTree<long,SqlValue> map)
        {
            if (map[defpos] is SqlValue sr)
                return sr;
            var reg = BTree<string, bool?>.Empty;
            var gfc = BTree<SqlValue, string>.Empty;
            var rem = BTree<SqlValue, string>.Empty;
            SqlValue sv = this;
            var an = alias ?? name;
            if (gs?.Has(this)==true)
            {
                var gfc0 = BTree<SqlValue, string>.Empty;
                var reg0 = BTree<string, bool?>.Empty;
                sv = _ColsForRestView(dp, gf, gs, ref gfc0, ref rem, ref reg0, ref map);
                gfc+=(new Selector(an,dp,nominalDataType,(int)gfc.Count), an);
                reg +=(an, true);
            }                
            else
            {
                sv = _ColsForRestView(dp, gf, gs, ref gfc, ref rem, ref reg, ref map);
                an = sv.alias ?? sv.name;
            }
            for (var b = gfc.First(); b != null; b = b.Next())
                gf = gf.Add(b.key(), b.value());
            if ((!isConstant) || gf.QuerySpec()?.tableExp.group?.Has(sv) == true)
                for (var b=rem.First();b!=null;b=b.Next())
                    gf+=(Query.Source, gf.source+(b.key(),b.value()));
            var cs = gf.source as CursorSpecification;
            var rg = cs.restGroups;
            for (var b = reg.First(); b != null; b = b.Next())
                if (!rg.Contains(b.key()))
                    rg += (b.key(), (int)rg.Count);
            cs += (CursorSpecification.RestGroups, rg);
            gf += (Query.Source, cs);
            return sv;
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

        internal virtual bool Grouped(GroupSpecification gs)
        {
            return gs?.Has(this)==true;
        }
        /// <summary>
        /// If this value contains an aggregator, set the regiser for it.
        /// If not, return null and the caller will make a Literal.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        internal virtual SqlValue SetReg(RowBookmark rb)
        {
            return null;
        }

        internal override Basis New(BTree<long, object> m)
        {
            throw new NotImplementedException();
        }
    }
    /// <summary>
    /// A TYPE value for use in CAST
    /// </summary>
    internal class SqlTypeExpr : SqlValue
    {
        internal static readonly long
            TreatType = --_uid; // Domain
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
    }
    /// <summary>
    /// A Subtype value for use in TREAT
    /// </summary>
    internal class SqlTreatExpr : SqlValue
    {
        internal const long
            TreatExpr = -323; // SqlValue
        SqlValue val => (SqlValue)mem[TreatExpr];
        /// <summary>
        /// constructor: a new Treat expression
        /// </summary>
        /// <param name="ty">the type</param>
        /// <param name="cx">the context</param>
        internal SqlTreatExpr(long dp,SqlValue v,Domain ty, Context cx)
            : base(dp,BTree<long,object>.Empty+(NominalType,(ty.kind==Sqlx.ONLY && ty.iri!=null)?
                  new Domain(v.nominalDataType.kind,v.nominalDataType.mem+(Domain.Iri,ty.iri)):ty)
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
        internal override SqlValue _Setup(Transaction tr,Context cx,Query q,Domain d)
        {
            return Setup(tr,cx,q,val,nominalDataType);
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlTreatExpr)base.Replace(cx, so, sv);
            var v = r.val.Replace(cx,so,sv);
            if (v != r.val)
                r += (TreatExpr, v);
            cx.done += (defpos, r);
            return r;
        }
        /// <summary>
        /// The value had better fit the specified type
        /// </summary>
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            if (val.Eval(tr,cx)?.NotNull() is TypedValue tv)
            {
                if (!nominalDataType.HasValue(tv))
                    throw new DBException("2200G", nominalDataType.ToString(), val.ToString()).ISO();
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
                if (!nominalDataType.HasValue(tv))
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
        internal static readonly long
            Field = --_uid; // string
        internal string field => (string)mem[Field];
        internal SqlField(long defpos,Domain dt,SqlValue lf,Ident s) 
            : base(defpos,BTree<long,object>.Empty
                  +(NominalType,dt)+(Field,s.ident)+(Left,lf))
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
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Field:"); sb.Append(field);
            return base.ToString();
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
            Kind = -324, //Sqlx
            Modifier = -325; // Sqlx
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
            : base(dp, (mm ?? BTree<long, object>.Empty) + (NominalType, _Type(dp, op, m, lf, rg))
                  + (Left, lf) + (Right, rg) + (Modifier, m) + (Kind, op)
                  +(Dependents,BList<long>.Empty+(lf?.defpos??-1)+(rg?.defpos??-1))
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
        Domain _Type(Domain d, SqlValue sv)
        {
            return (d == Domain.Null && sv != null) ? sv.nominalDataType : d;
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlValueExpr)base.Replace(cx, so, sv);
            var lf = r.left.Replace(cx, so, sv);
            if (lf != r.left)
                r += (Left, lf);
            var rg = r.right.Replace(cx, so, sv);
            if (rg != r.right)
                r += (Right, rg);
            cx.done += (defpos, r);
            return r;
        }

        internal override BTree<long, SqlValue> Disjoin()
        { // parsing guarantees right associativity
            return (kind == Sqlx.AND)? right.Disjoin()+(left.defpos, left):base.Disjoin();
        }
        /// <summary>
        /// analysis stage Selects(): setup the operands
        /// </summary>
        /// <param name="q">The required data type</param>
        /// <param name="s">a default value</param>
        internal override SqlValue _Setup(Transaction tr, Context cx, Query q, Domain d)
        {
            var m = mem;
            switch (kind)
            {
                case Sqlx.AND:
                    CheckType(d, Domain.Bool);
                    m = m + (Left, left._Setup(tr, cx, q, d)) + (Right, right._Setup(tr, cx, q, d));
                    break;
                case Sqlx.ASC:
                    CheckType(d, Domain.UnionNumeric);
                    m += (Left, left._Setup(tr, cx, q, d)); break;// JavaScript;
                case Sqlx.ASSIGNMENT:
                    {
                        m = m + (Left, left._Setup(tr, cx, q, Domain.Content));
                        var r = right._Setup(tr, cx, q, left.nominalDataType);
                        m += (Right, r);
                        d = _Type(d, r);
                        break;
                    }
                case Sqlx.BOOLEAN:
                    CheckType(d, Domain.Bool);
                    m += (Left, left._Setup(tr, cx, q, Domain.Bool)); break;
                case Sqlx.CALL:
                    CheckType(Domain.JavaScript, Domain.ArgList);
                    m = m + (Left, left._Setup(tr, cx, q, Domain.JavaScript))
                    + (Right, right._Setup(tr, cx, q, Domain.ArgList));
                    break;
                case Sqlx.COLLATE:
                    CheckType(d, Domain.Char);
                    m = m + (Left, left._Setup(tr, cx, q, Domain.Char))
                    + (Right, right._Setup(tr, cx, q, Domain.Char));
                    break;
                case Sqlx.COLON: // JavaScript
                    {
                        var lf = left._Setup(tr, cx, q, Domain.Content);
                        m = m + (Left, lf)
                        + (Right, right._Setup(tr, cx, q, left.nominalDataType));
                        d = _Type(d, lf);
                        break;
                    }
                case Sqlx.COMMA: // JavaScript
                    m = m + (Left, left._Setup(tr, cx, q, Domain.Char))
                    + (Right, right._Setup(tr, cx, q, Domain.Content));
                    break;
                case Sqlx.CONCATENATE: goto case Sqlx.COLLATE;
                case Sqlx.CONTAINS:
                    CheckType(d, Domain.Bool);
                    m = m + (Left, left._Setup(tr, cx, q, Domain.Period))
                    + (Right, right._Setup(tr, cx, q, Domain.Content)); // Can be UnionDate or left.nominalDataType
                    break;
                case Sqlx.DESC: goto case Sqlx.ASC;
                case Sqlx.DIVIDE: goto case Sqlx.TIMES;
                case Sqlx.DOT:
                    d = right.nominalDataType;
                    m = m + (Left, left._Setup(tr, cx, q, Domain.Content))
                        + (Right, right._Setup(tr, cx, q, d));
                    break;
                case Sqlx.EQL:
                    CheckType(d, Domain.Bool);
                    m = m + (Left, left._Setup(tr, cx, q, Domain.Content))
                    + (Right, right._Setup(tr, cx, q, left.nominalDataType));
                    break;
                case Sqlx.EQUALS: goto case Sqlx.OVERLAPS;
                case Sqlx.EXCEPT: goto case Sqlx.UNION;
                case Sqlx.GEQ: goto case Sqlx.EQL;
                case Sqlx.GTR: goto case Sqlx.EQL;
                case Sqlx.INTERSECT: goto case Sqlx.UNION;
                case Sqlx.LBRACK:
                    {
                        var lf = left._Setup(tr, cx, q, Domain.Array);
                        CheckType(d, left.nominalDataType.elType);
                        d = left.nominalDataType.elType;
                        m = m + (Left, lf) + (Right, right._Setup(tr, cx, q, Domain.Int));
                        break;
                    }
                case Sqlx.LEQ: goto case Sqlx.EQL;
                case Sqlx.LOWER: goto case Sqlx.UPPER; //JavaScript >>
                case Sqlx.LSS: goto case Sqlx.EQL;
                case Sqlx.MINUS:
                    if (left == null)
                    {
                        m += (Right, right._Setup(tr, cx, q, d));
                        d = right.nominalDataType;
                        break;
                    }
                    goto case Sqlx.PLUS;
                case Sqlx.NEQ: goto case Sqlx.EQL;
                case Sqlx.NO:
                    m += (Left, left._Setup(tr, cx, q, d));
                    d = _Type(d, left);
                    break;
                case Sqlx.NOT:
                    CheckType(d, Domain.Bool);
                    m += (Left, left._Setup(tr, cx, q, d)); break;
                case Sqlx.OR: goto case Sqlx.AND;
                case Sqlx.OVERLAPS:
                    CheckType(d, Domain.Bool);
                    m = m + (Left, left._Setup(tr, cx, q, Domain.UnionDate))
                    + (Right, right._Setup(tr, cx, q, left.nominalDataType));
                    break;
                case Sqlx.PERIOD:
                    if (d == Domain.Content || d == Domain.Value)
                        d = Domain.UnionDate;
                    m = m + (Left, left._Setup(tr, cx, q, d))
                    + (Right, right._Setup(tr, cx, q, left.nominalDataType));
                    break;
                case Sqlx.PLUS:
                    {
                        Domain nt = FindType(Domain.UnionDateNumeric);
                        if (nt.kind == Sqlx.INTERVAL)
                        {
                            m = m + (Left, left._Setup(tr, cx, q, left.nominalDataType))
                            + (Right, right._Setup(tr, cx, q, right.nominalDataType));
                        }
                        else if (nt.kind == Sqlx.DATE || nt.kind == Sqlx.TIME || nt.kind == Sqlx.TIMESTAMP)
                        {
                            m = m + (Left, left._Setup(tr, cx, q, left.nominalDataType))
                            + (Right, right._Setup(tr, cx, q, Domain.Interval));
                        }
                        else
                        {
                            CheckType(d, nt);
                            m = m + (Left, left._Setup(tr, cx, q, nt))
                            + (Right, right._Setup(tr, cx, q, nt));
                            d = nt;
                        }
                    }
                    break;
                case Sqlx.PRECEDES: goto case Sqlx.OVERLAPS;
                case Sqlx.QMARK: // JavaScript
                    {
                        m += (Left, left._Setup(tr, cx, q, Domain.Bool));
                        var r = right._Setup(tr, cx, q, Domain.Content);
                        m += (Right, r);
                        d = _Type(d, r);
                        break;
                    }
                case Sqlx.RBRACK:
                    m = m + (Left, left._Setup(tr, cx, q, new Domain(Sqlx.ARRAY, d)))
                    + (Right, right._Setup(tr, cx, q, Domain.Int));
                    break;
                case Sqlx.SET:
                    d = _Type(d, left);
                    break;
                case Sqlx.SUCCEEDS: goto case Sqlx.OVERLAPS;
                case Sqlx.TIMES:
                    d = Domain.UnionNumeric;
                    CheckType(d, Domain.UnionNumeric);
                    m = m + (Left, left._Setup(tr, cx, q, Domain.UnionNumeric))
                    + (Right, right._Setup(tr, cx, q, Domain.UnionNumeric));
                    break;
                case Sqlx.UNION:
                    {
                        var lf = left._Setup(tr, cx, q, Domain.Collection);
                        m = m + (Left, lf)
                        + (Right, right._Setup(tr, cx, q, left.nominalDataType));
                        d = _Type(d, lf);
                        break;
                    }
                case Sqlx.UPPER:
                    CheckType(Domain.Int, Domain.Int); // JvaScript shift <<
                    m = m + (Left, left._Setup(tr, cx, q, Domain.Int))
                    + (Right, right._Setup(tr, cx, q, Domain.Int));
                    break;
                case Sqlx.XMLATTRIBUTES: goto case Sqlx.COLLATE;
                case Sqlx.XMLCONCAT: goto case Sqlx.COLLATE;
                default:
                    throw new DBException("22005V", d.ToString(),
                        (q.rowType ?? Domain.Content).ToString()).ISO()
                        .AddType(d);
            }
            return new SqlValueExpr(defpos, m + (NominalType, d));
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
            if (alias != null)
                return new Selector(alias, defpos, nominalDataType,(int)q.cols.Count);
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
            Query gf, GroupSpecification gs, ref BTree<SqlValue, string> gfc, 
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
                    st = new Selector(nn, dp, nominalDataType, 0);
                    rem += (new SqlValueExpr(defpos, kind, rel.First().key(), rer.First().key(), mod,
                        new BTree<long, object>(_Alias, nn)), nn);
                    gfc += (st, nn);
                    map += (defpos, st);
                    return st;
                case 2: // lr: QS->Cleft op right as exp; CS->Left’ as Cleft 
                    // rel will have just one entry, rer will have 0 entries
                    se = new SqlValueExpr(defpos, kind,
                        new Selector(nl, dp, left.nominalDataType,0), right, mod,
                        new BTree<long, object>(_Alias, alias));
                    rem += (rel.First().key(), nl);
                    gfc += (gfl.First().key(), nl);
                    map += (defpos, se);
                    return se;
                case 3:// rr: QS->Left op Cright as exp; CS->Right’ as CRight
                    // rer will have just one entry, rel will have 0 entries
                    se = new SqlValueExpr(defpos, kind, left,
                        new Selector(er.alias ?? er.name, dp, 
                        right.nominalDataType, 0),
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
                st = new Selector(nn, dp, se.nominalDataType, 0);
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
        internal override SqlValue SetReg(RowBookmark rb)
        {
            left?.SetReg(rb);
            right?.SetReg(rb);
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
        internal override Query DistributeConditions(Transaction tr,Context cx, Query q, BTree<SqlValue, SqlValue> repl)
        {
            q = base.DistributeConditions(tr,cx, q, repl);
            switch (kind)
            {
                case Sqlx.NO: return left.DistributeConditions(tr,cx, q, repl);
                case Sqlx.AND:
                    q = left.DistributeConditions(tr,cx, q, repl);
                    return right.DistributeConditions(tr,cx, q, repl);
                case Sqlx.EQL:
                    if (IsFrom(q, false))
                        return q.AddCondition(tr,cx,new BTree<long, SqlValue>(defpos, this), null, null);
                    else if (repl[left] is SqlValue lr && lr.IsFrom(q, false))
                    {
                        var ns = new SqlValueExpr(defpos, kind, repl[left], right, Sqlx.NO);
                        return q.AddCondition(tr,cx, new BTree<long, SqlValue>(ns.defpos, ns), null,null);
                    }
                    else if (repl[right] is SqlValue rl && rl.IsFrom(q, false))
                    {
                        var ns = new SqlValueExpr(defpos, kind, repl[right], left, Sqlx.NO);
                        return q.AddCondition(tr,cx, new BTree<long, SqlValue>(ns.defpos, ns), null,null);
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
        internal override SqlValue PartsIn(Domain dt)
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
                                    nominalDataType.defaultValue :
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
                                return nominalDataType.defaultValue;
                            var w = v.dataType.Eval(v, Sqlx.ADD, new TInt(1L));
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
                            Domain ct = left.nominalDataType;
                            if (ct.kind == Sqlx.CHAR)
                            {
                                var b = right.Eval(tr, cx)?.NotNull();
                                if (b == null)
                                    return null;
                                string cname = b?.ToString();
                                if (ct.culture.Name == cname)
                                    return left.Eval(tr, cx)?.NotNull();
                                Domain nt = new Domain(ct.kind, BTree<long, object>.Empty
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
                            if (left.nominalDataType.kind == Sqlx.ARRAY
                                && right.nominalDataType.kind == Sqlx.ARRAY)
                                return left.nominalDataType.Concatenate((TArray)left.Eval(tr, cx),
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
                                return nominalDataType.defaultValue;
                            if (right.nominalDataType.kind == Sqlx.PERIOD)
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
                                return nominalDataType.defaultValue;
                            var w = v.dataType.Eval(v, Sqlx.MINUS, new TInt(1L));
                            cx.row.values += (defpos, v);
                            return (mod == Sqlx.BEFORE) ? w : v;
                        }
                    case Sqlx.DIVIDE:
                        v = nominalDataType.Eval(left.Eval(tr, cx)?.NotNull(), kind,
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
                            return left.nominalDataType.Coerce(
                                TMultiset.Except(ta, tb, mod == Sqlx.ALL));
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
                            return left.nominalDataType.Coerce(
                                TMultiset.Intersect(ta, tb, mod == Sqlx.ALL));
                        }
                    case Sqlx.LBRACK:
                        {
                            var al = left.Eval(tr, cx)?.NotNull();
                            var ar = right.Eval(tr, cx)?.NotNull();
                            if (al == null || ar == null)
                                return null;
                            var sr = ar.ToInt();
                            if (al.IsNull || !sr.HasValue)
                                return nominalDataType.defaultValue;
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
                                return nominalDataType.defaultValue;
                            var s = (byte)(or.ToLong().Value & 0x1f);
                            if (mod == Sqlx.GTR)
                                unchecked
                                {
                                    a = (long)(((ulong)ol.Val()) >> s);
                                }
                            else
                            {
                                if (ol.IsNull)
                                    return nominalDataType.defaultValue;
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
                                v = right.nominalDataType.Eval(new TInt(0), Sqlx.MINUS, tb);
                                cx.row.values += (defpos, v);
                                return v;
                            }
                            var ta = left.Eval(tr, cx)?.NotNull();
                            if (ta == null)
                                return null;
                            v = left.nominalDataType.Eval(ta, kind, tb);
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
                                            return nominalDataType.defaultValue;
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
                                return nominalDataType.defaultValue;
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
                            v = left.nominalDataType.Eval(ta, kind, tb);
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
                                return nominalDataType.defaultValue;
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
                                return nominalDataType.defaultValue;
                            var b = ((bool)lf.Val()) ? a.left : a.right;
                            return a.left.nominalDataType.Coerce(b.Eval(tr, cx));
                        }
                    case Sqlx.RBRACK:
                        {
                            var a = left.Eval(tr, cx)?.NotNull();
                            var b = right.Eval(tr, cx)?.NotNull();
                            if (a == null || b == null)
                                return null;
                            if (a.IsNull || b.IsNull)
                                return nominalDataType.defaultValue;
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
                                return nominalDataType.defaultValue;
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
                            v = nominalDataType.Eval(ta, kind, tb);
                            cx.row.values += (defpos, v);
                            return v;
                        }
                    case Sqlx.UNION:
                        {
                            var ta = left.Eval(tr, cx)?.NotNull();
                            var tb = right.Eval(tr, cx)?.NotNull();
                            if (ta == null || tb == null)
                                return null;
                            return left.nominalDataType.Coerce(
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
                                return nominalDataType.defaultValue;
                            var s = (byte)(or.ToLong().Value & 0x1f);
                            if (lf.IsNull)
                                return nominalDataType.defaultValue;
                            a = lf.ToLong().Value >> s;
                            v = new TInt(a);
                            cx.row.values += (defpos, v);
                            return v;
                        }
                    //       case Sqlx.XMLATTRIBUTES:
                    //         return new TypedValue(left.nominalDataType, BuildXml(left) + " " + BuildXml(right));
                    case Sqlx.XMLCONCAT:
                        {
                            var ta = left.Eval(tr, cx)?.NotNull();
                            var tb = right.Eval(tr, cx)?.NotNull();
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
        static Domain _Type(long dp, Sqlx kind, Sqlx mod, SqlValue left, SqlValue right)
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
            if (e == null || (nominalDataType != null && nominalDataType != v.nominalDataType))
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
        internal override Query Conditions(Transaction tr,Context cx, Query q, bool disj, out bool move)
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
                        q = left.Conditions(tr,cx, q, false, out _);
                        q = right.Conditions(tr,cx, q, false, out _);
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
                        q = left.Conditions(tr,cx, q, false, out _);
                        break;
                    }
            }
            if (q != null && nominalDataType == Domain.Bool)
                DistributeConditions(tr,cx, q, BTree<SqlValue, SqlValue>.Empty);
            move = false;
            return q;
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
        /// <summary>
        /// constructor for null
        /// </summary>
        /// <param name="cx">the context</param>
        internal SqlNull(long dp = -1)
            : base(dp,new BTree<long,object>(NominalType,Domain.Null))
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
        internal override Query Conditions(Transaction tr, Context cx, Query q, bool disj,out bool move)
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
            _Val = -326;// TypedValue
        internal TypedValue val=>(TypedValue)mem[_Val];
        /// <summary>
        /// Constructor: a Literal
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="ty">the kind of literal</param>
        /// <param name="v">the value of the literal</param>
        public SqlLiteral(long dp, TypedValue v) : base(dp, BTree<long, object>.Empty
            + (NominalType, v.dataType) + (_Val, v))
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
            + (NominalType, dt) + (_Val, dt.defaultValue))
        { }
        internal override Query Conditions(Transaction tr, Context cx, Query q, bool disj,out bool move)
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
            if (c == null || (nominalDataType != null && nominalDataType != v.nominalDataType))
                return false;
            return val == c.val;
        }
        /// <summary>
        /// Get the literal value
        /// </summary>
        /// <returns>the value</returns>
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            return val ?? nominalDataType.defaultValue;
        }
        internal override SqlValue _Setup(Transaction tr,Context cx,Query q,Domain d)
        {
            if (d.kind != Sqlx.Null && (val == null || !val.dataType.EqualOrStrongSubtypeOf(d)))
                return this+ (_Val, d.Coerce(val));
            return this;
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
            return val.ToString();
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
            : base(dp, op.Parse(n))
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
        internal TableRow rec => (TableRow)mem[TableRow];
        public SqlTableRowStart(long dp, Query f, TableRow r) : base(dp,BTree<long,object>.Empty
            +(NominalType,f.rowType)+(_From,f)+(TableRow,r))
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
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            var table = from as Table;
            return table.versionedRows[rec.ppos].start ?? TNull.Value;
        }
    }
    /// <summary>
    /// A Row value
    /// </summary>
    internal class SqlRow : SqlValue
    {
        internal static readonly long
            Columns = --_uid; // BList<SqlValue>
        internal BList<SqlValue> columns => 
            (BList<SqlValue>)mem[Columns]?? BList<SqlValue>.Empty;
        public SqlRow(long dp, BTree<long, object> m) : base(dp, m) { }
        /// <summary>
        /// A row from the parser
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="r">the row</param>
        public SqlRow(long dp, Domain t, BList<SqlValue> r)
            : base(dp, BTree<long,object>.Empty
                  +(NominalType,t)+(Columns,r)+(Dependents,_Deps(r))+(Depth,1+_Depth(r)))
        {
            if ((int)r.Count != t.Length)
                throw new DBException("22207");
        }
        public static SqlRow operator+(SqlRow s,(long,object)m)
        {
            return new SqlRow(s.defpos, s.mem + m);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlRow(defpos, m);
        }
        internal SqlValue this[int i] =>columns[i];
        internal int Length => (int)columns.Count;
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlRow)base.Replace(cx, so, sv);
            var cs = r.columns;
            for (var b=cs.First();b!=null;b=b.Next())
            {
                var v = b.value().Replace(cx, so, sv);
                if (v != b.value())
                    cs += (b.key(), (SqlValue)v);
            }
            if (cs != r.columns)
                r += (Columns, cs);
            cx.done += (defpos, r);
            return r;
        }
        /// <summary>
        /// the value
        /// </summary>
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            var r = new TypedValue[nominalDataType.Length];
            for (int i = 0; i < nominalDataType.Length; i++)
                if (nominalDataType.columns[i].domain.Coerce(this[i].Eval(tr,cx)?.NotNull()) 
                    is TypedValue v)
                    r[i] = v;
                else
                    return null;
            return new TRow(nominalDataType, r);
        }
        internal override bool aggregates()
        {
            for (var i = 0; i < columns.Count; i++)
                if (columns[i].aggregates())
                    return true;
            return false;
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            for (var i = 0; i < columns.Count; i++)
                columns[i].Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx,RowSet rs)
        {
            for (var i = 0; i < columns.Count; i++)
                columns[i].StartCounter(_cx,rs);
        }
        internal override void _AddIn(Context _cx,RowBookmark rb, ref BTree<long, bool?> aggsDone)
        {
            for (var i = 0; i < columns.Count; i++)
                columns[i]._AddIn(_cx,rb, ref aggsDone);
        }
        internal override SqlValue SetReg(RowBookmark rb)
        {
            var nulls = true;
            for (var i = 0; i < columns.Count; i++)
                if (columns[i].SetReg(rb) != null)
                    nulls = false;
            return nulls ? null : this;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = "(";
            for (var i=0;i<Length;i++)
            {
                var c = nominalDataType.columns[i];
                sb.Append(cm); cm = ",";
                sb.Append(c.domain.name + "=");sb.Append(columns[i]);
            }
            sb.Append(")");
            return sb.ToString();
        }
    }
    internal class SqlRowArray : SqlValue
    {
        internal static readonly long
            Rows = --_uid; // BList<SqlRow>
        internal BList<SqlRow> rows =>
            (BList<SqlRow>)mem[Rows]?? BList<SqlRow>.Empty;
        public SqlRowArray(long dp, Domain dt, BList<SqlRow> rs) : base(dp, BTree<long, object>.Empty
            + (NominalType,dt) + (Rows, rs))
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlRowArray)base.Replace(cx, so, sv);
            var rws = r.rows;
            for (var b=r.rows.First();b!=null;b=b.Next())
            {
                var v = b.value().Replace(cx, so, sv);
                if (v != b.value())
                    rws += (b.key(), (SqlRow)v);
            }
            if (rws != r.rows)
                r += (Rows, rws);
            cx.done += (defpos, r);
            return r;
        }
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            var r = new TArray(nominalDataType, (int)rows.Count);
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
        internal override SqlValue SetReg(RowBookmark rb)
        {
            var nulls = true;
            for (var i = 0; i < rows.Count; i++)
                if (rows[i].SetReg(rb) != null)
                    nulls = false;
            return nulls ? null : this;
        }
    }
    /// <summary>
    /// This class is only used during aggregation computations
    /// </summary>
    internal class GroupRow : SqlRow
    {
        internal const long
            Info = -327; // BList<SqlValue>
        internal BList<SqlValue> info =>
            (BList<SqlValue>)mem[Info]?? BList<SqlValue>.Empty;
        internal GroupRow(Context _cx, long dp, GroupingRowSet.GroupInfo gi,
            GroupingRowSet.GroupingBookmark gb, Domain gt, TRow key)
            : base(dp, BTree<long, object>.Empty
                  + (Info, gi.group.members) + (NominalType, gt)
                  + (Columns, _Columns(_cx,dp,gi,gb, key))) { }
        static BList<SqlValue> _Columns(Context _cx, long dp,GroupingRowSet.GroupInfo gi,
            GroupingRowSet.GroupingBookmark gb,TRow key)
        {
            var dt = ((GroupingRowSet)gb._rs).qry.rowType;
            var columns = BList<SqlValue>.Empty;
            for (int j = 0; j < dt.Length; j++) // not grs.rowType!
            {
                var c = dt.columns[j];
                var sc = gi.grs.qry.cols[j];
                columns += (key[c.name] is TypedValue tv) ? new SqlLiteral(dp,tv)
                        : sc.SetReg(gb) ?? new SqlLiteral(dp,sc.Eval(_cx,gb) ?? TNull.Value);
            }
            return columns;
/*            for (var b = gi.grs.having.First(); b != null; b = b.Next())
                b.value().AddIn(gi.grs);
            gi.grs.g_rows+=(key, this); */
        }
        /// <summary>
        /// the value
        /// </summary>
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            var r = new TypedValue[nominalDataType.Length];
            for (int i = 0; i < nominalDataType.Length; i++)
                if (nominalDataType.columns[i].domain.Coerce(this[i].Eval(tr,cx)?.NotNull()) 
                    is TypedValue v)
                    r[i] = v;
                else
                    return null;
            return new TRow(nominalDataType,r);
        }
    }
    // a document value (field keys are constant strings, values are expressions)
    internal class SqlDocument : SqlValue
    {
        internal static readonly long
            Document = --_uid; // BList<(string,SqlValue)> // can be parsed as SqlValues
        internal BList<(string, SqlValue)> document =>
            (BList<(string, SqlValue)>)mem[Document] ?? BList<(string, SqlValue)>.Empty;
        public SqlDocument(long dp, BTree<long, object> m) : base(dp, m+(NominalType,Domain.Document)) { }
        public static SqlDocument operator+(SqlDocument s,(long,object)x)
        {
            return new SqlDocument(s.defpos, s.mem + x);
        }
        public static SqlDocument operator+(SqlDocument s,(string,SqlValue)x)
        {
            var dt = s.nominalDataType;
            dt = new Domain(dt.columns 
                + (dt.Length, new Selector(x.Item1,-1,x.Item2.nominalDataType,dt.Length)));
            return s + (Document, s.document + (s.Length, x))
                + (NominalType,dt);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlDocument(defpos,m);
        }
        internal int Length => (int)document.Count; 
        public SqlValue this[string n] =>(nominalDataType.names[n] is Selector s)?
                    document[s.seq].Item2:null;
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlDocument)base.Replace(cx, so, sv);
            var doc = r.document;
            for (var b=doc.First();b!=null;b=b.Next())
            {
                var v = b.value().Item2.Replace(cx, so, sv);
                if (v != b.value().Item2)
                    doc += (b.key(), (b.value().Item1,(SqlValue)v));
            }
            if (doc != r.document)
                r += (Document, doc);
            cx.done += (defpos, r);
            return r;
        }
        /// <summary>
        /// the value
        /// </summary>
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            var d = new TDocument();
            for(var b=document.First();b!=null;b=b.Next())
            {
                var (n, s) = b.value();
                d.Add(n, s.Eval(tr, cx));
            }
            return d;
        }
        public override string ToString()
        {
            var sb = new StringBuilder("{");
            var cm = "";
            for (var i = 0; i < Length; i++)
            {
                var (n, s) = document[i];
                sb.Append(cm); cm = ",";
                sb.Append(n); sb.Append(":");
                sb.Append(s);
            }
            sb.Append("}");
            return sb.ToString();
        }
    }
    internal class SqlDocArray : SqlDocument
    {
        internal const long
            Docs = -328; // BList<SqlDocument>
        internal BList<SqlDocument> docs =>
            (BList<SqlDocument>)mem[Docs] ?? BList<SqlDocument>.Empty;
        public SqlDocArray(long dp,BTree<long,object>m) : base(dp,m+(NominalType,Domain.DocArray))
        { }
        public static SqlDocArray operator+(SqlDocArray s,(long,object)x)
        {
            return new SqlDocArray(s.defpos, s.mem + x);
        }
        public static SqlDocArray operator +(SqlDocArray s, SqlDocument x)
        {
            return s +(Docs,s.docs+x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlDocArray(defpos,m);
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlDocArray)base.Replace(cx, so, sv);
            var ds = r.docs;
            for (var b=ds.First();b!=null;b=b.Next())
            {
                var v = b.value().Replace(cx, so,sv);
                if (v != b.value())
                    ds += (b.key(), (SqlDocument)v);
            }
            if (ds != r.docs)
                r += (Docs, ds);
            cx.done += (defpos, r);
            return r;
        }
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            var r = new TDocArray();
            for (var b = docs.First(); b != null; b = b.Next())
                r.Add(b.value().Eval(tr, cx));
            return r;
        }
        internal override void RowSet(Transaction tr, Context cx, Query f)
        {
              cx.rb = new DocArrayRowSet(tr, cx, f, this).First(cx);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("[");
            sb.Append(docs);
            sb.Append("]");
            return sb.ToString();

        }
    }
    internal class SqlXmlValue : SqlValue
    {
        internal const long
            Attrs = -329, // BTree<int,(XmlName,SqlValue)>
            Children = -330, // BList<SqlXmlValue>
            Content = -331, // SqlValue
            Element = -332; // XmlName
        public XmlName element => (XmlName)mem[Element];
        public BList<(XmlName, SqlValue)> attrs =>
            (BList<(XmlName, SqlValue)>)mem[Attrs] ?? BList<(XmlName, SqlValue)>.Empty;
        public BList<SqlXmlValue> children =>
            (BList<SqlXmlValue>)mem[Children]?? BList<SqlXmlValue>.Empty;
        public SqlValue content => (SqlValue)mem[Content]; // will become a string literal on evaluation
        public SqlXmlValue(long dp, XmlName n, SqlValue c, BTree<long, object> m) 
            : base(dp, m + (NominalType, Domain.XML)+(Element,n)+(Content,c)) { }
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlXmlValue)base.Replace(cx, so, sv);
            var at = r.attrs;
            for (var b=at.First();b!=null;b=b.Next())
            {
                var v = b.value().Item2.Replace(cx, so, sv);
                if (v != b.value().Item2)
                    at += (b.key(), (b.value().Item1, (SqlValue)v));
            }
            if (at != r.attrs)
                r += (Attrs, at);
            var co = r.content.Replace(cx,so,sv);
            if (co != r.content)
                r += (Content, co);
            var ch = r.children;
            for(var b=ch.First();b!=null;b=b.Next())
            {
                var v = b.value().Replace(cx,so,sv);
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
            for(var b=attrs.First();b!=null;b=b.Next())
                if (b.value().Item2.Eval(tr,cx)?.NotNull() is TypedValue ta)
                    r.attributes+=(b.value().Item1.ToString(), ta);
            for(var b=children.First();b!=null;b=b.Next())
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
            ArrayValuedQE = -333; // QueryExpression
        public QueryExpression aqe => (QueryExpression)mem[ArrayValuedQE];
        public SqlSelectArray(long dp, QueryExpression qe, BTree<long, object> m = null)
            : base(dp, (m ?? BTree<long, object>.Empty
                  + (NominalType, qe.rowType) + (ArrayValuedQE, qe))) { }
        protected SqlSelectArray(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlSelectArray operator+(SqlSelectArray s,(long,object)x)
        {
            return new SqlSelectArray(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlSelectArray(defpos, m);
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlSelectArray)base.Replace(cx, so, sv);
            var ae = r.aqe.Replace(cx,so,sv);
            if (ae != r.aqe)
                r += (ArrayValuedQE, ae);
            cx.done += (defpos, r);
            return r;
        }
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            var va = new TArray(nominalDataType);
            var ars = aqe.RowSets(tr,cx);
            int j = 0;
            var nm = aqe.name;
            for (var rb=ars.First(cx);rb!= null;rb=rb.Next(cx))
            {
                var rw = rb.row;
                if (nominalDataType.elType.Length == 0)
                    va[j++] = rw[nm];
                else
                {
                    var vs = new TypedValue[aqe.display];
                    for (var i = 0; i < aqe.display; i++)
                        vs[i] = rw[i];
                    va[j++] = new TRow(nominalDataType.elType, vs);
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
        internal override SqlValue SetReg(RowBookmark rb)
        {
            aqe.SetReg(rb);
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
            Array = -334, // BList<SqlValue>
            Svs = -335; // SqlValueSelect
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
        public SqlValueArray(long dp, Domain t,BList<SqlValue> v)
            : base(dp, BTree<long,object>.Empty
                  +(NominalType,new Domain(Sqlx.ARRAY, t))+(Array,v))
        { }
        public SqlValueArray(long dp, Domain t, SqlValueSelect sv)
            : base(dp, BTree<long, object>.Empty
                 + (NominalType, t) + (Svs, sv))
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
        /// <summary>
        /// analysis stage Selects(): Setup the array elements
        /// </summary>
        /// <param name="q">The required data type or null</param>
        /// <param name="s">the defaullt select</param>
        /// <returns>the possibly new SqlValue</returns>
        internal override SqlValue _Setup(Transaction tr,Context cx,Query q,Domain d)
        {
            var t = nominalDataType.elType ?? Domain.Value;
            if (svs != null)
                return this+(Svs,svs._Setup(tr, cx, q, d));
            var r = this;
            for (int j = 0; j < array.Count; j++)
            {
                r = (SqlValueArray)Setup(tr,cx,q,array[j], t);
                t = t.LimitBy(array[j].nominalDataType);
            }
            if (t!=nominalDataType.elType)
                r += (NominalType,new Domain(Sqlx.ARRAY,t));
            return r;
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlValueArray)base.Replace(cx, so, sv);
            var ar = r.array;
            for (var b=ar.First();b!=null;b=b.Next())
            {
                var v = b.value().Replace(cx, so, sv);
                if (v != b.value())
                    ar += (b.key(), (SqlValue)v);
            }
            if (ar != r.array)
                r += (Array, ar);
            var ss = r.svs.Replace(cx,so,sv);
            if (ss != r.svs)
                r += (Svs, ss);
            cx.done += (defpos, r);
            return r;
        }
        /// <summary>
        /// evaluate the array
        /// </summary>
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            if (svs != null)
            {
                var ar = new List<TypedValue>();
                var ers = svs.Eval(tr, cx) as TRowSet;
                for (var b = ers.rowSet.First(cx); b != null; b = b.Next(cx))
                    ar.Add(b.row[0]);
                return new TArray(nominalDataType, ar);
            }
            var a = new TArray(nominalDataType,(int)array.Count);
            for (int j = 0; j < array.Count; j++)
                a[j] = array[j]?.Eval(tr,cx)?.NotNull() ?? nominalDataType.defaultValue;
            return a;
        }
        internal override TypedValue Eval(Context _cx, RowBookmark bmk)
        {
            var rs = bmk._rs;
            var tr = rs._tr;
            if (svs != null)
            {
                var ar = new List<TypedValue>();
                var ers = svs.Eval(_cx,bmk) as TRowSet;
                for (var b = ers.rowSet.First(_cx); b != null; b = b.Next(_cx))
                    ar.Add(b.row[0]);
                return new TArray(nominalDataType, ar);
            }
            var a = new TArray(nominalDataType, (int)array.Count);
            for (int j = 0; j < array.Count; j++)
                a[j] = array[j]?.Eval(_cx,bmk)?.NotNull() ?? nominalDataType.defaultValue;
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
        internal override SqlValue SetReg(RowBookmark rb)
        {
            var nulls = true;
            if (array != null)
                for (var i = 0; i < array.Count; i++)
                    if (array[i].SetReg(rb) != null)
                        nulls = false;
            if (svs?.SetReg(rb) != null)
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
            Expr = -336, // Query
            Source = -337, // string
            TargetType = -338; // Domain
        /// <summary>
        /// the subquery
        /// </summary>
        public Query expr =>(Query)mem[Expr];
        public Domain targetType => (Domain)mem[TargetType];
        public string source => (string)mem[Source];
        /// <summary>
        /// constructor: a subquery
        /// <param name="q">the context</param>
        /// <param name="t">the rowset type</param>
        /// <param name="s">the query source</param>
        /// </summary>
        public SqlValueSelect(long dp, Query q, Domain t, string s)
            : base(dp,BTree<long,object>.Empty
                  +(NominalType,(t.kind!=Sqlx.Null)?t:q.rowType)
                  +(Expr,q)+(Source,s)+(TargetType,t))
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlValueSelect)base.Replace(cx, so, sv);
            var ex = r.expr.Replace(cx,so,sv);
            if (ex != r.expr)
                r += (Expr, ex);
            cx.done += (defpos, r);
            return r;
        }
        internal override Query Conditions(Transaction tr, Context cx, Query q, bool disj,out bool move)
        {
            move = false;
            return expr.Conditions(tr, cx, q);
        }
        internal override TypedValue Eval(Transaction tr, Context cx)
        {
            var ers = expr.RowSets(tr,cx);
            if (targetType?.kind == Sqlx.TABLE)
                return new TRowSet(ers ?? EmptyRowSet.Value);
            var rb = ers.First(cx);
            if (rb == null)
                return nominalDataType.defaultValue;
            TypedValue tv = rb._rs.qry.cols[0].Eval(tr,cx)?.NotNull();
            if (targetType != null)
                tv = targetType.Coerce(tv);
            return tv;
        }
        /// <summary>
        /// analysis stage Selects(): setup the results of the subquery
        /// </summary>
        /// <param name="q">The required data type or null</param>
        internal override SqlValue _Setup(Transaction tr,Context cx,Query q,Domain d)
        {
            return this+(IsSetup,true)+(TargetType,targetType ?? d);
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
            cx.rb = f.RowSets(tr,cx).First(cx);
        }
        internal override void StartCounter(Context _cx, RowSet rs)
        {
            expr.StartCounter(_cx,rs);
        }
        internal override void _AddIn(Context _cx,RowBookmark rb, ref BTree<long, bool?> aggsDone)
        {
            expr.AddIn(_cx,rb);
        }
        internal override SqlValue SetReg(RowBookmark rb)
        {
            expr.SetReg(rb);
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
            Bits = -339; // BList<long>
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
            : base(dp, BTree<long, object>.Empty + (NominalType, Domain.Bool)+ (Bits, c)) { }
        protected ColumnFunction(long dp, BTree<long, object> m) :base(dp, m) { }
        public static ColumnFunction operator+(ColumnFunction s,(long,object)x)
        {
            return new ColumnFunction(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ColumnFunction(defpos,mem);
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
            return "GROUPING(..)";
        }
    }
    internal class SqlCursor : SqlValue
    {
        internal const long
            Spec = -340; // CursorSpecification
        internal CursorSpecification spec=>(CursorSpecification)mem[Spec];
        internal SqlCursor(long dp, CursorSpecification cs, string n) 
            : base(dp, BTree<long,object>.Empty+
                  (NominalType,cs.rowType)+(Name, n)+(Dependents,new BList<long>(cs.defpos))
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlCursor)base.Replace(cx, so, sv);
            var sp = r.spec.Replace(cx,so,sv);
            if (sp != r.spec)
                r += (Spec, sp);
            cx.done += (defpos, r);
            return r;
        }
        internal override TypedValue Eval(Context _cx, RowBookmark rb) 
        {
            return new TCursor(rb);
        }
        public override string ToString()
        {
            return "Cursor "+name.ToString();
        }
    }
    internal class SqlCall : SqlValue
    {
        internal const long
            Call = -341; // CallStatement
        public CallStatement call =>(CallStatement)mem[Call];
        public SqlCall(long dp, CallStatement c, string n, BTree<long,object>m=null)
            : base(dp, m??BTree<long, object>.Empty + (NominalType, c.returnType)
                  + (Name, n) + (Call, c)+(Dependents,new BList<long>(c.defpos))
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlCall)base.Replace(cx, so, sv);
            var ca = r.call.Replace(cx,so,sv);
            if (ca != r.call)
                r += (Call, ca);
            cx.done += (defpos, r);
            return r;
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
            for (var i = 0; i < call.parms.Count; i++)
                if (call.parms[i].IsFrom(rs.qry))
                    call.parms[i].Build(_cx,rs);
        }
        internal override void StartCounter(Context _cx, RowSet rs)
        {
            for (var i = 0; i < call.parms.Count; i++)
                if (call.parms[i].IsFrom(rs.qry))
                    call.parms[i].StartCounter(_cx, rs);
        }
        internal override void _AddIn(Context _cx,RowBookmark rb, ref BTree<long, bool?> aggsDone)
        {
            for (var i = 0; i < call.parms.Count; i++)
                if (call.parms[i].IsFrom(rb._rs.qry))
                    call.parms[i]._AddIn(_cx,rb, ref aggsDone);
        }
        internal override SqlValue SetReg(RowBookmark rb)
        {
            var nulls = true;
            for (var i = 0; i < call.parms.Count; i++)
                if (call.parms[i].SetReg(rb) != null)
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
        public SqlProcedureCall(Context cx, long dp, CallStatement c)
            : base(dp, c, c.name)
        {
            Resolve(cx,null);
        }
        protected SqlProcedureCall(long dp,BTree<long,object>m):base(dp,m) { }
        public static SqlProcedureCall operator+(SqlProcedureCall s,(long,object)x)
        {
            return new SqlProcedureCall(s.defpos, s.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new SqlProcedureCall(defpos, m);
        }
        /// <summary>
        /// analysis stage Selects(): set up the operands
        /// </summary>
        /// <param name="q">The required data type or null</param>
        internal override SqlValue _Setup(Transaction tr,Context cx,Query q,Domain d)
        {
            var r = this;
            if (call.proc == null)
            {
                r += (Call,call+(CallStatement.Proc,tr.role.procedures[call.name]?[(int)call.parms.Count]));
                if (r.call.proc == null)
                {
                    UDType ut = call.var.nominalDataType as UDType;
                    if (ut == null)
                        throw new DBException("42108", ((Selector)call.var).name).Mix();
                    r += (Call,call+(CallStatement.RetType, d)
                        +(CallStatement.Proc,ut.methods[call.name]?[(int)call.parms.Count]));
                }
            }
            if (r.call.proc != null)
            {
                //              if (call.proc.Returns(call.database) == null)
                //              new Parser(transaction).ReparseFormals(call.database,call.proc);
                r +=(NominalType,call.proc.retType);
            }
            return r;
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
                    ((tv != TNull.Value)?tv:nominalDataType.defaultValue):null;
            }
            catch (DBException e)
            {
                throw e;
            }
            catch (Exception)
            {
                return nominalDataType.defaultValue;
            }
        }
        internal override void Eqs(Transaction tr,Context cx,ref Adapters eqs)
        {
            if (tr.role.objects[call.proc.inverse] is Procedure inv)
                eqs = eqs.Add(call.proc.defpos, call.parms[0].defpos, call.proc.defpos, inv.defpos);
            base.Eqs(tr,cx,ref eqs);
        }
        internal override int ColFor(Query q)
        {
            if (call.parms.Count == 1)
                return call.parms[0].ColFor(q);
            return base.ColFor(q);
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
        public SqlMethodCall(long dp, CallStatement c, string n) : base(dp,c,n)
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
        /// <summary>
        /// analysis stage Selects(): setup the method call so it can be evaluated
        /// </summary>
        /// <param name="q">The required data type or null</param>
        internal override SqlValue _Setup(Transaction tr,Context cx,Query q,Domain d)
        {
            var r = this;
   //         if (call.var != null && call.var is Selector)
   //             call.var = (call.var.name.scope as Query)?.defs[call.var.name];
            if (call.proc == null)
            {
                var v = call.var;
                if (v != null)
                {
                    var ut = v.nominalDataType as UDType;
                    if (ut == null)
                        throw new DBException("42108", v.nominalDataType.name).Mix();
                    r += (Call,call+(CallStatement.Proc,ut.methods[call.name]?[(int)call.parms.Count]));
                }
            }
            if (r.call.proc != null)
            {
                var rk = call.proc.retType;
                if (rk.Constrain(d) == null)
                    throw new DBException("42161", d, rk).Mix();
                r +=(Call,call+(CallStatement.RetType,rk));
                r += (NominalType,rk);
            }
            return r;
        }
        /// <summary>
        /// Evaluate the method call and return the result
        /// </summary>
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            if (call.var == null)
                throw new PEException("PE241");
            return (((Method)call.proc).Exec(tr, cx, call.var, call.parms) is TypedValue tv) ?
                ((tv != TNull.Value) ? tv : nominalDataType.defaultValue):null;
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
            Sce = -342, //SqlRow
            Udt = -343; // UDType
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
            : base(dp, c,c.proc.name,new BTree<long,object>(Udt,u))
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlConstructor)base.Replace(cx, so, sv);
            var sc = r.sce.Replace(cx, so, sv);
            if (sc != r.sce)
                r += (Sce, sc);
            cx.done += (defpos, r);
            return r;
        }
        /// <summary>
        /// Analysis stage Selects(): setup the constructor call so that we can call it
        /// </summary>
        /// <param name="q">the context</param>
        /// <param name="q">The required data type or null</param>
        /// <param name="s">a default select to use</param>
        /// <returns></returns>
        internal override SqlValue _Setup(Transaction t, Context cx,Query q, Domain d)
        {
            return this+(Sce,new SqlRow(defpos,d,BList<SqlValue>.Empty));
        }
        /// <summary>
        /// evaluate the constructor and return the new object
        /// </summary>
        internal override TypedValue Eval(Transaction tr, Context cx)
        {
            return (((Method)call.proc).Exec(tr, cx,null,call.parms) is TypedValue tv) ?
                ((tv != TNull.Value) ? tv : nominalDataType.defaultValue) : null;
        }
        public override string ToString()
        {
            return call.name+"(..)";
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
                  +(SqlConstructor.Sce,new SqlRow(dp,u,ins))
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlDefaultConstructor)base.Replace(cx, so, sv);
            var sc = r.sce.Replace(cx, so, sv);
            if (sc != r.sce)
                r += (SqlConstructor.Sce, sc);
            cx.done += (defpos, r);
            return r;
        }
        /// <summary>
        /// Evaluate the default constructor
        /// </summary>
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            var dt = ut;
            try
            {
                var obs = new TypedValue[dt.Length];
                for (int i = 0; i < dt.Length; i++)
                    obs[i] = sce.columns[i].Eval(tr,cx);
                return new TRow(dt, obs);
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
            return nominalDataType.name+"(..)";
        }
     }
    /// <summary>
    /// A built-in SQL function
    /// </summary>
    internal class SqlFunction : SqlValue
    {
        internal const long
            Filter = -344, //SqlValue
            Kind = -345, // Sqlx
            Mod = -346, // Sqlx
            Monotonic = -347, // bool
            Op1 = -348, // SqlValue
            Op2 = -349, // SqlValue
            Query = -350,//Query
            _Val = -351,//SqlValue
            Window = -352, // WindowSpecification
            WindowId = -353; // long
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
            Domain dt=null,BTree<long,object>mm=null) : 
            base(dp,(mm??BTree<long,object>.Empty)+(NominalType,_Type(f,vl,o1,dt))
                +(Name,f.ToString())+(Kind,f)+(Mod,m)+(_Val,vl)+(Op1,o1)+(Op2,o2)
                +(Dependents,BList<long>.Empty+vl.defpos+o1.defpos+o2.defpos)
                +(Depth,1+_Max(vl.depth,o1.depth,o2.depth)))
        { }
        /// <summary>
        /// Constructor: a function SqlValue from the parser
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="f">the function name</param>
        public SqlFunction(long dp, Sqlx f, Domain dt, SqlValue vl, SqlValue o1, SqlValue o2, Sqlx m,
            BTree<long, object> mm = null) :
            base(dp, (mm??BTree<long, object>.Empty) + (NominalType, dt)
                + (Name, f.ToString()) + (Kind, f) + (Mod, m) + (_Val, vl) + (Op1, o1) 
                + (Op2, o2))
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlFunction)base.Replace(cx, so, sv);
            var fi = r.filter?.Replace(cx, so, sv);
            if (fi != r.filter)
                r += (Filter, fi);
            var o1 = r.op1?.Replace(cx, so, sv);
            if (o1 != r.op1)
                r += (Op1, o1);
            var o2 = r.op2?.Replace(cx, so, sv);
            if (o2 != r.op2)
                r += (Op2, o2);
            var vl = r.val?.Replace(cx, so, sv);
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
        /// analysis stage Selects(): set up operands
        /// </summary>
        /// <param name="d">The required data type or null</param>
        /// <param name="s">a default select</param>
        /// <returns></returns>
        internal override SqlValue _Setup(Transaction tr,Context cx,Query q,Domain d)
        {
            var r = this;
            var tx = query as TableExpression;
            var qs = query as QuerySpecification;
            if (qs != null)
                tx = qs.tableExp;
            if (tx != null && mem.Contains(WindowId))
            {
                if (tx.window[windowId] == null)
                    throw new DBException("42161", windowId).Mix();
                r += (Window,tx.window[windowId]);
            }
            if (query == null && cx.cur is Query qu)
                r += (Query,qu);
            switch (kind)
            {
                case Sqlx.ABS:
                    return r+(_Val,Setup(tr,cx,q,val,FindType(Domain.UnionNumeric))); 
                case Sqlx.ANY:
                    return r+(_Val,Setup(tr, cx,q, val, Domain.Bool));
                case Sqlx.AVG:
                    return r+(_Val,Setup(tr, cx, q, val, FindType(Domain.UnionNumeric)));
                case Sqlx.ARRAY:
                    return r+(_Val,Setup(tr, cx, q, val, Domain.Collection));  // Mongo $push
                case Sqlx.CARDINALITY:
                    return r+(_Val,Setup(tr, cx, q, val, Domain.Int)); ;
                case Sqlx.CASE:
                    return r+(_Val,Setup(tr, cx, q, val, d))
                    +(Op1,Setup(tr, cx, q, op1,val.nominalDataType))
                    +(Op2,Setup(tr, cx, q, op2,val.nominalDataType)); 
                case Sqlx.CAST:
                    return r +(_Val,Setup(tr, cx, q, val,val.nominalDataType))
                    +(Monotonic,((SqlTypeExpr)op1).type.kind != Sqlx.CHAR 
                    && val.nominalDataType.kind != Sqlx.CHAR);
                case Sqlx.CEIL: goto case Sqlx.CEILING;
                case Sqlx.CEILING:
                    return r+(_Val,Setup(tr, cx, q, val,FindType(Domain.UnionNumeric)));
                case Sqlx.CHAR_LENGTH:
                    return r+(_Val,Setup(tr, cx, q, val,Domain.Char)); 
                case Sqlx.CHARACTER_LENGTH: goto case Sqlx.CHAR_LENGTH;
                case Sqlx.CHECK: break;
                case Sqlx.COLLECT:
                    return r+(_Val,Setup(tr, cx, q, val,Domain.Collection));
                case Sqlx.COUNT:
                    return r+(_Val,Setup(tr, cx, q, val,Domain.Int));
                case Sqlx.CURRENT_DATE: 
                case Sqlx.CURRENT_TIME: 
                case Sqlx.CURRENT_TIMESTAMP: return r + (Monotonic, true);
                case Sqlx.ELEMENT:
                    return r+(_Val,Setup(tr, cx, q, val,Domain.Collection));
                case Sqlx.EXP:
                    return r+(_Val,Setup(tr, cx, q, val, Domain.Real))+(Monotonic,true);
                case Sqlx.EVERY: goto case Sqlx.ANY;
                case Sqlx.EXTRACT:
                    return r+(_Val,Setup(tr,cx,q,val,Domain.UnionDate))
                        +(Monotonic,mod == Sqlx.YEAR);
                case Sqlx.FLOOR: goto case Sqlx.CEIL;
                case Sqlx.FIRST:
                    return r+(_Val,Setup(tr, cx, q, val, Domain.Content)); // Mongo
                case Sqlx.FUSION: goto case Sqlx.COLLECT;
                case Sqlx.INTERSECTION: goto case Sqlx.COLLECT;
                case Sqlx.LAST: goto case Sqlx.FIRST;
                case Sqlx.SECURITY:
                    if (cx.user.defpos != tr.owner)
                        throw new DBException("42105", this);
                    return r+(_Val,Setup(tr, cx, q, val, Domain._Level));
                case Sqlx.LN: goto case Sqlx.EXP;
                case Sqlx.LOCALTIME: goto case Sqlx.CURRENT_TIME;
                case Sqlx.LOCALTIMESTAMP: goto case Sqlx.CURRENT_TIMESTAMP;
                case Sqlx.LOWER:
                    return r+(_Val,Setup(tr, cx, q, val,Domain.Char));
                case Sqlx.MAX:
                    return r+(_Val,Setup(tr, cx, q, val,Domain.Content));
                case Sqlx.MIN: goto case Sqlx.MAX;
                case Sqlx.MOD:
                    return r+(Op1,Setup(tr, cx, q, op1,FindType(Domain.UnionNumeric)))
                    +(Op2,Setup(tr, cx, q, op2,op1.nominalDataType));
                case Sqlx.NEXT: break;
                case Sqlx.NORMALIZE:
                    return r+(_Val,Setup(tr, cx, q, val,Domain.Char));
                case Sqlx.NULLIF:
                    return r+(Op1,Setup(tr, cx, q, op1,Domain.Content))
                    +(Op2,Setup(tr, cx, q, op2,op1.nominalDataType));
                case Sqlx.OCTET_LENGTH: goto case Sqlx.CHAR_LENGTH;
                case Sqlx.OVERLAY: goto case Sqlx.NORMALIZE;
                case Sqlx.POSITION:
                    r+=(Op1,Setup(tr,cx, q, op1, Domain.Char));
                    if (op1!=null)
                        r+=(Op2,Setup(tr, cx, q, op2, op1.nominalDataType));
                    return r;
                case Sqlx.POWER: goto case Sqlx.EXP;
                case Sqlx.PROVENANCE: break;
                case Sqlx.RANK: 
                    return r+(_Val,Setup(tr,cx,q,val,FindType(Domain.UnionNumeric)));
                case Sqlx.ROW_NUMBER: break;
                case Sqlx.SET: goto case Sqlx.COLLECT;
                case Sqlx.SOME: goto case Sqlx.ANY;
                case Sqlx.STDDEV_POP:
                    return r+(_Val,Setup(tr,cx,q,val, Domain.Real));
                case Sqlx.STDDEV_SAMP: goto case Sqlx.STDDEV_POP;
                case Sqlx.SUBSTRING:
                    return r+(_Val,Setup(tr,cx, q, val,Domain.Char))
                    +(Op1,Setup(tr, cx,q, op1,Domain.Int))
                    +(Op2,Setup(tr,cx, q, op2,Domain.Int));
                case Sqlx.SUM:
                    return r+(_Val,Setup(tr,cx, q, val,FindType(Domain.UnionNumeric)));
                case Sqlx.TRANSLATE:
                    return r+(_Val,Setup(tr, cx, q, val,Domain.Char));
                case Sqlx.TRIM:
                    return r+(_Val,Setup(tr, cx, q, val,Domain.Char))
                    +(Op1,Setup(tr,cx, q, op1,val.nominalDataType));
                case Sqlx.TYPE_URI:
                    return r+(Op1,Setup(tr, cx,q, op1,Domain.Content));
                case Sqlx.UPPER: goto case Sqlx.LOWER;
                case Sqlx.WHEN:
                    return r+(_Val,Setup(tr, cx, q, val,d))
                    +(Op1,Setup(tr, cx, q, op1,Domain.Bool))
                    +(Op2,Setup(tr, cx, q, op2,val.nominalDataType));
                case Sqlx.XMLCAST: goto case Sqlx.CAST;
                case Sqlx.XMLAGG: goto case Sqlx.LOWER;
                case Sqlx.XMLCOMMENT: goto case Sqlx.LOWER;
                case Sqlx.XMLPI: goto case Sqlx.LOWER;
                //      case Sqlx.XMLROOT: goto case Sqlx.LOWER;
                case Sqlx.XMLQUERY:
                    return r+(Op1,Setup(tr,cx, q, op1,Domain.Char))
                    +(Op2,Setup(tr,cx, q, op2,op1.nominalDataType));
            }
            return this;
        }
        /// <summary>
        /// conditions processing
        /// </summary>
        /// <param name="q">the context</param>
        internal override Query Conditions(Transaction tr,Context cx, Query q, bool disj,out bool move)
        {
            move = false;
            var f = Setup(tr,cx, q, this, Domain.Bool);
            cx.Replace(this,f);
            return (Query)cx.done[defpos];
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
            +(WindowSpecification.OrdType, window.order.KeyType(q.rowType, window.partition))
            +(WindowSpecification.PartitionType,window.order.KeyType(q.rowType, 0, window.partition)));
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            if (window == null || _cx.func.Contains(defpos))
                return;
            var fd = new FunctionData();
            _cx.func += (defpos, fd);
            fd.building = true;
            fd.regs = new CTree<TRow, FunctionData.Register>(window.partitionType);
            for (var b = rs.First(_cx);b!=null;b=b.Next(_cx))
            {
                PRow ks = null;
                for (var i = window.partition - 1;i>=0; i--)
                    ks = new PRow(window.order.items[i].what.Eval(_cx,b),ks);
                var pkey = new TRow(window.partitionType, ks);
                fd.cur = fd.regs[pkey];
                ks = null;
                for (var i = (int)window.order.items.Count - 1; i >= window.partition; i--)
                    ks = new PRow(window.order.items[i].what.Eval(_cx,b), ks);
                var okey = new TRow(window.ordType, ks);
                var worder = OrderSpec.Empty;
                var its = BTree<int, OrderItem>.Empty;
                for (var i = window.partition; i < window.order.items.Count; i++)
                    its+=((int)its.Count,window.order.items[i]);
                worder = worder+ (OrderSpec.Items, its) + (OrderSpec._KeyType,window.ordType);
                if (fd.cur ==null)
                {
                    fd.cur = new FunctionData.Register();
                    fd.regs+=(pkey, fd.cur);
                    fd.cur.wrs = new OrderedRowSet(_cx,rs.qry, rs, worder, false);
                    fd.cur.wrs.tree = new RTree(rs, new TreeInfo(window.ordType, TreeBehaviour.Allow, TreeBehaviour.Allow));
                }
                var dt = rs.qry.rowType;
                var vs = new TypedValue[dt.Length];
                for (var i = 0; i < dt.Length; i++)
                    vs[i] = rs.qry.cols[i].Eval(_cx,b);
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
            return (v is SqlFunction f && (nominalDataType == null || nominalDataType == v.nominalDataType)) &&
             MatchExp(q,val, f.val) && MatchExp(q,op1, f.op1) && MatchExp(q,op2, f.op2);
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
                case Sqlx.COUNT: return Domain.Int;
                case Sqlx.CURRENT: return Domain.Bool; // for syntax check: CURRENT OF
                case Sqlx.CURRENT_DATE: return Domain.Date;
                case Sqlx.CURRENT_TIME: return Domain.Timespan;
                case Sqlx.CURRENT_TIMESTAMP: return Domain.Timestamp;
                case Sqlx.ELEMENT: return val.nominalDataType.elType;
                case Sqlx.FIRST: return Domain.Content;
                case Sqlx.EXP: return Domain.Real;
                case Sqlx.EVERY: return Domain.Bool;
                case Sqlx.EXTRACT: return Domain.Int;
                case Sqlx.FLOOR: return Domain.UnionNumeric;
                case Sqlx.FUSION: return Domain.Collection;
                case Sqlx.INTERSECTION: return Domain.Collection;
                case Sqlx.LAST: return Domain.Content;
                case Sqlx.SECURITY: return Domain._Level;
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
                case Sqlx.SUM: return Domain.UnionNumeric;
                case Sqlx.TRANSLATE: return Domain.Char;
                case Sqlx.TYPE_URI: return Domain.Char;
                case Sqlx.TRIM: return Domain.Char;
                case Sqlx.UPPER: return Domain.Char;
                case Sqlx.VERSIONING: return Domain.Int;
                case Sqlx.WHEN: return val.nominalDataType;
                case Sqlx.XMLCAST: return op1.nominalDataType;
                case Sqlx.XMLAGG: return Domain.Char;
                case Sqlx.XMLCOMMENT: return Domain.Char;
                case Sqlx.XMLPI: return Domain.Char;
                case Sqlx.XMLQUERY: return Domain.Char;
            }
            return dt;
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
                    ks = new PRow(window.order.items[i].what.Eval(cx,rb), ks);
                fd.cur = fd.regs[new TRow(window.partitionType,ks)];
                fd.cur.Clear();
                fd.cur.wrs.building = false; // ? why should it be different?
                ks = null;
                for (var i = (int)window.order.items.Count - 1; i >= window.partition; i--)
                    ks = new PRow(window.order.items[i].what.Eval(cx,rb), ks);
                var dt = rb._rs.qry.rowType;
                for (var b = firstTie = fd.cur.wrs.PositionAt(cx,ks); b != null; b = b.Next(cx))
                {
                    for (var i=0;i<dt.Length;i++)
                    {
                        var c = dt.columns[i];
                        var n = c.name;
                        if (rb.row[i] is TypedValue tv && c.domain.Compare(tv,b.row[i]) != 0)
                            goto skip;
                    }
                    fd.cur.wrb = b;
                    break;
                    skip:;
                }
                for (var b = fd.cur.wrs.First(cx); b != null; b = b.Next(cx))
                    if (InWindow(cx,b))
                        switch (window.exclude)
                        {
                            case Sqlx.NO:
                                AddIn(cx,b);
                                break;
                            case Sqlx.CURRENT:
                                if (b._pos != fd.cur.wrb._pos)
                                    AddIn(cx,b);
                                break;
                            case Sqlx.TIES:
                                if (!Ties(fd.cur.wrb, b))
                                    AddIn(cx,b);
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
                                return new TReal((w < 0.0) ? -w : w);
                            }
                        case Sqlx.NUMERIC:
                            {
                                Common.Numeric w = (Numeric)v.Val();
                                return new TNumeric((w < Numeric.Zero) ? -w : w);
                            }
                        case Sqlx.UNION:
                            {
                                var cs = val.nominalDataType.columns;
                                for (int i = 0; i < cs.Count; i++)
                                    if (cs[i].domain.kind == Sqlx.INTEGER)
                                        goto case Sqlx.INTEGER;
                                for (int i = 0; i < cs.Count; i++)
                                    if (cs[i].domain.kind == Sqlx.NUMERIC)
                                        goto case Sqlx.NUMERIC;
                                for (int i = 0; i < cs.Count; i++)
                                    if (cs[i].domain.kind == Sqlx.REAL)
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
                        fd.cur.acc = new TArray(new Domain(Sqlx.ARRAY, fd.cur.mset.tree?.First()?.key().dataType));
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
                        return nominalDataType.defaultValue;
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
                            if (f.op1.nominalDataType.Compare(f.op1.Eval(tr,cx), v) == 0)
                                return f.val.Eval(tr,cx);
                            f = fg;
                        }
                    }
                case Sqlx.CAST:
                    {
                        v = val?.Eval(tr,cx)?.NotNull();
                        if (v == null)
                            return null;
                        return nominalDataType.Coerce(v);
                    }
                case Sqlx.CEIL: goto case Sqlx.CEILING;
                case Sqlx.CEILING:
                    v = val?.Eval(tr,cx)?.NotNull();
                    if (v == null)
                        return null;
                    switch (val.nominalDataType.kind)
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
                            return nominalDataType.defaultValue;
                        if (v?.ToString().ToCharArray() is char[] chars)
                            return new TInt(chars.Length);
                        return new TInt(0);
                    }
                case Sqlx.CHARACTER_LENGTH: goto case Sqlx.CHAR_LENGTH;
             //   case Sqlx.CHECK: return new TRvv(rb);
                case Sqlx.COLLECT: return nominalDataType.Coerce((TypedValue)fd.cur.mset ??TNull.Value);
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
                        return nominalDataType.defaultValue;
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
                    switch (val.nominalDataType.kind)
                    {
                        case Sqlx.INTEGER:
                            return v;
                        case Sqlx.DOUBLE:
                            return new TReal(Math.Floor(v.ToDouble()));
                        case Sqlx.NUMERIC:
                            return new TNumeric(Common.Numeric.Floor((Common.Numeric)v.Val()));
                    }
                    break;
                case Sqlx.FUSION: return nominalDataType.Coerce(fd.cur.mset);
                case Sqlx.INTERSECTION:return nominalDataType.Coerce(fd.cur.mset);
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
                        return nominalDataType.defaultValue;
                    }
                case Sqlx.MAX: return fd.cur.acc;
                case Sqlx.MIN: return fd.cur.acc;
                case Sqlx.MOD:
                    if (op1 != null)
                        v = op1.Eval(tr,cx);
                    if (v.Val() == null)
                        return nominalDataType.defaultValue;
                    switch (op1.nominalDataType.kind)
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
                            return nominalDataType.defaultValue;
                        TypedValue b = op2.Eval(tr,cx)?.NotNull();
                        if (b == null)
                            return null;
                        if (b.IsNull || op1.nominalDataType.Compare(a, b) != 0)
                            return a;
                        return nominalDataType.defaultValue;
                    }
                case Sqlx.OCTET_LENGTH:
                    {
                        v = val?.Eval(tr,cx)?.NotNull();
                        if (v == null)
                            return null;
                        if (v.Val() is byte[] bytes)
                            return new TInt(bytes.Length);
                        return nominalDataType.defaultValue;
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
                            return nominalDataType.defaultValue;
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
                            return nominalDataType.defaultValue;
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
                        return m.Set();
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
                            return nominalDataType.defaultValue;
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
                            return nominalDataType.defaultValue;
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
                        return nominalDataType.defaultValue;
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
        }
        internal override SqlValue SetReg(RowBookmark rb)
        {
            if ((window!=null || aggregates0()))
            {
                cur = regs?[rb.key];
                if (cur == null)
                {
                    cur = new Register() { profile = rb.key };
                    if (regs == null)
                        regs = new CTree<TRow, Register>(rb.key.dataType);
                    regs+=(rb.key, cur);
                }
            } else
            {
                val.SetReg(rb);
                op1.SetReg(rb);
                op2.SetReg(rb);
            }
            return this;
        } */
        /// <summary>
        /// for aggregates and window functions we need to implement StartCounter
        /// </summary>
        internal override void StartCounter(Context _cx,RowSet rs)
        {
            var fd = _cx.func[defpos];
            if (fd == null)
                _cx.func += (defpos, new FunctionData());
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
                    fd.cur.mset = new TMultiset(val.nominalDataType);
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
                    fd.cur.acc = new TArray(new Domain(Sqlx.ARRAY, Domain.Content));
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
                            fd.cur.acc = new TArray(new Domain(Sqlx.ARRAY, val.nominalDataType));
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
                                fd.cur.mset = new TMultiset(val.nominalDataType);
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
             //               nominalDataType = fd.cur.acc.dataType;
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
                                fd.cur.mset = new TMultiset(val.nominalDataType.elType); // check??
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
                //            nominalDataType = cur.acc.dataType;
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
            var kt = val.nominalDataType;
            var wrv = fd.cur.wrb.row[n];
            TypedValue limit;
            if (window.high.current)
                limit = wrv;
            else if (window.high.preceding)
                limit = kt.Eval(wrv, (kt.AscDesc == Sqlx.ASC) ? Sqlx.MINUS : Sqlx.PLUS, window.high.distance);
            else
                limit = kt.Eval(wrv, (kt.AscDesc == Sqlx.ASC) ? Sqlx.PLUS : Sqlx.MINUS, window.high.distance);
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
            var kt = val.nominalDataType;
            var tv = fd.cur.wrb.row[n];
            TypedValue limit;
            if (window.low.current)
                limit = tv;
            else if (window.low.preceding)
                limit = kt.Eval(tv, (kt.AscDesc != Sqlx.DESC) ? Sqlx.PLUS : Sqlx.MINUS, window.low.distance);
            else
                limit = kt.Eval(tv, (kt.AscDesc != Sqlx.DESC) ? Sqlx.MINUS : Sqlx.PLUS, window.low.distance);
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
                var n = oi.what.defpos;
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
        internal override bool Check(Transaction tr,GroupSpecification group)
        {
            if (aggregates0())
                return false;
            return base.Check(tr,group);
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
        /// <summary>
        /// See notes on SqlHttpUsing class: we are generating columns for the contrib query.
        /// </summary>
        /// <param name="gf">The query: From with a RestView target</param>
        /// <returns></returns>
        internal override SqlValue _ColsForRestView(long dp, Query gf, GroupSpecification gs, 
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
                        var c0 = new SqlFunction(dp, Sqlx.SUM, val, null, null, Sqlx.NO,null,
                            new BTree<long,object>(_Alias,n0));
                        var c1 = new SqlFunction(dp, Sqlx.COUNT, val, null, null, Sqlx.NO,null,
                            new BTree<long, object>(_Alias, n1));
                        rem = rem+(c0, n0)+(c1, n1);
                        var s0 = new Selector(dp, BTree<long,object>.Empty
                            +(NominalType,nominalDataType)+(Name,n0)+(Query,gf));
                        var s1 = new Selector(dp, BTree<long, object>.Empty
                            + (NominalType, Domain.Int) + (Name, n1) + (Query, gf));
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
                        var ct = new SqlFunction(dp, mod, val, null, null, Sqlx.NO, Domain.Int,
                        new BTree<long,object>(_Alias,an));
                        SqlValue st = ct;
                        rem+=(ct, an);
                        st = new Selector(dp, BTree<long, object>.Empty
                            + (NominalType, Domain.Int) + (Name, an) + (Query, gf));
                        gfc+=(st, an);
                        map+=(defpos, st);
                        return st;
                    }
                case Sqlx.STDDEV_POP:
                    {
                        var n0 = ac;
                        var n1 = "D_" + defpos;
                        var n2 = "E_" + defpos;
                        var c0 = new SqlFunction(dp, Sqlx.SUM, val, null, null, Sqlx.NO,null,
                            new BTree<long,object>(_Alias,n0));
                        var c1 = new SqlFunction(dp, Sqlx.COUNT, val, null, null, Sqlx.NO,null,
                            new BTree<long,object>(_Alias,n1));
                        var c2 = new SqlFunction(dp, Sqlx.SUM,
                            new SqlValueExpr(dp, Sqlx.TIMES, val, val, Sqlx.NO), null, null, Sqlx.NO,null,
                        new BTree<long, object>(_Alias,n2));
                        rem = rem+(c0, n0)+(c1, n1)+(c2, n2);
                        // c0 is SUM(x), c1 is COUNT, c2 is SUM(X*X)
                        // SQRT((c2-2*c0*xbar+xbar*xbar)/c1)
                        var s0 = new Selector(dp, BTree<long, object>.Empty
                            + (NominalType, nominalDataType) + (Name, n0) + (Query, gf));
                        var s1 = new Selector(dp, BTree<long, object>.Empty
                            + (NominalType, nominalDataType) + (Name, n1) + (Query, gf));
                        var s2 = new Selector(dp, BTree<long, object>.Empty
                            + (NominalType, nominalDataType) + (Name, n2) + (Query, gf));
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
                            var vt = val?.nominalDataType;
                            var vn = ac;
                            if (kind == Sqlx.COUNT)
                            {
                                nk = Sqlx.SUM;
                                vt = Domain.Int;
                            }
                            var st = this + (_Alias,ac);
                            rem+=(st, ac);
                            var va = new Selector(dp, BTree<long,object>.Empty
                                +(NominalType,vt)+(Name,vn)+(Query, gf));
                            var sf = new SqlFunction(dp, nk, nominalDataType,
                                va, op1, op2, mod,new BTree<long,object>(_Alias,an));
                            gfc+=(va, vn);
                            map+=(defpos, sf);
                            return sf;
                        }
                        if (aggregates())
                        {
                            var sr = new SqlFunction(dp, kind, nominalDataType,
                                val._ColsForRestView(dp, gf, gs, ref gfc, ref rem, ref reg, ref map),
                                op1, op2, mod, new BTree<long, object>(_Alias, an));
                            map+=(defpos, sr);
                            return sr;
                        }
                        var r = this+(_Alias,an);
                        gfc+=(r, an);
                        rem+=(r, an);
                        var sn = new Selector(dp,BTree<long,object>.Empty
                            +(Query,gf)+(NominalType,nominalDataType)+(Name,an));
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
    internal class SqlCoalesce : SqlFunction
    {
        internal SqlCoalesce(long dp, SqlValue op1, SqlValue op2)
            : base(dp, Sqlx.COALESCE, Domain.Content,null, op1, op2, Sqlx.NO) { }
        protected SqlCoalesce(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlCoalesce operator+(SqlCoalesce s,(long,object)x)
        {
            return new SqlCoalesce(s.defpos, s.mem + x);
        }
        internal override SqlValue _Setup(Transaction tr,Context cx,Query q,Domain d)
        {
            var r = this + (Op1, Setup(tr, cx, q, op1, d));
            r += (NominalType, r.op1.nominalDataType); // don't combine these!
            return r+(Op2,Setup(tr,cx,q,op2, nominalDataType));
        }
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            return (op1.Eval(tr,cx) is TypedValue lf) ? 
                ((lf == TNull.Value) ? op2.Eval(tr,cx) : lf) : null;
        }
        public override string ToString()
        {
            return "COALESCE(..)";
        }
    }
    internal class SqlTypeUri : SqlFunction
    {
        internal SqlTypeUri(long dp, SqlValue op1)
            : base(dp, Sqlx.TYPE_URI, Domain.Char, null, op1, null, Sqlx.NO) { }
        protected SqlTypeUri(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlTypeUri operator+(SqlTypeUri s,(long,object)x)
        {
            return new SqlTypeUri(s.defpos, s.mem + x);
        }
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            TypedValue v = null;
            if (op1 != null)
                v = op1.Eval(tr,cx);
            if (v==null || v.IsNull)
                return nominalDataType.defaultValue;
            var st = v.dataType;
            if (st.iri != null)
                return v;
 /*           var d = LocalTrans(op1);
            if (d == null)
                return nominalDataType.defaultValue;
            long td = st.DomainDefPos(d,-1);
            if (td > 0)
            {
                var dt = d.GetDataType(td);
                if (dt != null)
                    return new TChar( dt.iri);
            } */
            return nominalDataType.defaultValue;
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
            All = -354, // bool
            Between = -355, // bool
            Found = -356, // bool
            High = -357, // SqlValue
            Low = -358, // SqlValue
            Op = -359, // Sqlx
            Select = -360, //QuerySpecification
            Vals = -361, //BList<SqlValue>
            What = -362, //SqlValue
            Where = -363; // SqlValue
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
            : base(defpos,BTree<long,object>.Empty+(NominalType,Domain.Bool)
            + (What,w)+(Op,o)+(All,a)+(Select,s)
                  +(Dependents,BList<long>.Empty+w.defpos+s.defpos)
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
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (QuantifiedPredicate)base.Replace(cx, so, sv);
            var wh = r.what.Replace(cx, so, sv);
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
        /// Analysis stage Selects: setup the operands
        /// </summary>
        internal override SqlValue _Setup(Transaction tr,Context cx,Query q,Domain d)
        {
            return this+(What,Setup(tr,cx,q,what,d));
        }
        /// <summary>
        /// Analysis stage Conditions: process conditions
        /// </summary>
        internal override Query Conditions(Transaction tr,Context cx, Query q,bool disj,out bool move)
        {
            move = false;
            return what.Conditions(tr, cx, q, false, out _);
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
        internal override SqlValue SetReg(RowBookmark rb)
        {
            select.SetReg(rb);
            return (what.SetReg(rb)!=null)?this:null;
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
            : base(defpos,BTree<long,object>.Empty+(NominalType,Domain.Bool)
                  +(QuantifiedPredicate.What,w)+(QuantifiedPredicate.Between,b)
                  +(QuantifiedPredicate.Low,a)+(QuantifiedPredicate.High,h)
                  +(Dependents,BList<long>.Empty+w.defpos+a.defpos+h.defpos)
                  +(Depth,1+_Max(w.depth,a.depth,h.depth)))
        { }
        protected BetweenPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static BetweenPredicate operator+(BetweenPredicate s,(long,object)x)
        {
            return new BetweenPredicate(s.defpos, s.mem + x);
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (BetweenPredicate)base.Replace(cx, so, sv);
            var wh = r.what.Replace(cx, so, sv);
            if (wh != r.what)
                r += (QuantifiedPredicate.What, wh);
            var lw = r.low.Replace(cx, so, sv);
            if (lw != r.low)
                r += (QuantifiedPredicate.Low, lw);
            var hg = r.high.Replace(cx, so, sv);
            if (hg != r.high)
                r += (QuantifiedPredicate.High, hg);
            cx.done += (defpos, r);
            return r;
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
        internal override SqlValue SetReg(RowBookmark rb)
        {
            var a = what.SetReg(rb) != null;
            var b = low.SetReg(rb) != null;
            var c = high.SetReg(rb) != null;
            return (a || b || c) ? this : null; // avoid shortcut evaluation
        }
        /// <summary>
        /// Analysis stage Selects: setup operands
        /// </summary>
        internal override SqlValue _Setup(Transaction tr,Context cx,Query q,Domain d)
        {
            Domain dl = low.nominalDataType;
            if (dl == Domain.Int)
                dl = what.nominalDataType;
            return this+(QuantifiedPredicate.Low,Setup(tr, cx, q, low, dl))
            +(QuantifiedPredicate.High,Setup(tr, cx,q, high, dl))
            +(QuantifiedPredicate.Low,Setup(tr,cx,q,what, dl));
        }
        /// <summary>
        /// Analysis stage Conditions: support distribution of conditions to froms etc
        /// </summary>
        internal override Query Conditions(Transaction tr,Context cx,Query q,bool disj,out bool move)
        {
            move = false;
            q = what.Conditions(tr, cx, q, false,out _);
            if (low!=null)
                q = low.Conditions(tr, cx, q, false, out _);
            if (high!=null)
                q = high.Conditions(tr, cx, q, false, out _);
            return q;
        }
         /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            if (what.Eval(tr,cx) is TypedValue w)
            {
                var t = what.nominalDataType;
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
            return "BETWEEN ..";
        }
    }

    /// <summary>
    /// LikePredicate subclass of SqlValue
    /// </summary>
    internal class LikePredicate : SqlValue
    {
        internal const long
            Escape = -364, // SqlValue
            _Like = -365; // bool
        public SqlValue what => (SqlValue)mem[QuantifiedPredicate.What];
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
            : base(dp, new BTree<long,object>(NominalType,Domain.Bool)
                  +(Left,a)+(_Like,k)+(Right,b)+(Escape,e)
                  +(Dependents,BList<long>.Empty+a.defpos+b.defpos+e.defpos)
                  +(Depth,1+_Max(a.depth,b.depth,e.depth)))
        { }
        protected LikePredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static LikePredicate operator+(LikePredicate s,(long,object)x)
        {
            return new LikePredicate(s.defpos, s.mem + x);
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (LikePredicate)base.Replace(cx, so, sv);
            var wh = r.what.Replace(cx, so, sv);
            if (wh != r.what)
                r += (QuantifiedPredicate.What, wh);
            var esc = r.escape.Replace(cx, so, sv);
            if (esc != r.escape)
                r += (Escape, esc);
            cx.done += (defpos, r);
            return r;
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
        internal override SqlValue SetReg(RowBookmark rb)
        {
            return (left.SetReg(rb) != null || right.SetReg(rb) != null) ? this : null;
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
            : base(dp, new BTree<long, object>(NominalType, Domain.Bool)
                  +(QuantifiedPredicate.What,w)+(QuantifiedPredicate.Vals,vs)
                  +(Dependents,_Deps(vs)+w.defpos)+(Depth,1+_Max(w.depth,_Depth(vs))))
        {}
        protected InPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static InPredicate operator+(InPredicate s,(long,object)x)
        {
            return new InPredicate(s.defpos, s.mem + x);
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (InPredicate)base.Replace(cx, so, sv);
            var wh = r.what.Replace(cx, so, sv);
            if (wh != r.what)
                r += (QuantifiedPredicate.What, wh);
            var wr = r.where.Replace(cx, so, sv);
            if (wr != r.where)
                r += (QuantifiedPredicate.Select, wr);
            var vs = r.vals;
            for (var b=vs.First();b!=null;b=b.Next())
            {
                var v = b.value().Replace(cx, so, sv);
                if (v != b.value())
                    vs += (b.key(), (SqlValue)v);
            }
            if (vs != r.vals)
                r += (QuantifiedPredicate.Vals, vs);
            cx.done += (defpos, r);
            return r;
        }
        /// <summary>
        /// Analysis stage Conditions: check to see what conditions can be distributed
        /// </summary>
        internal override Query Conditions(Transaction tr, Context cx, Query q, bool disj, out bool move)
        {
            move = false;
            q = what?.Conditions(tr, cx, q,false, out _);
            if (vals != null)
                for(var v = vals.First();v!=null;v=v.Next())
                    q = v.value().Conditions(tr, cx, q, false, out _);
            return q;
        }
        /// <summary>
        /// Evaluate the predicate for the current row
        /// </summary>
        internal override TypedValue Eval(Transaction tr,Context cx)
        {
            if (what.Eval(tr,cx) is TypedValue w)
            {
                if (vals != null)
                {
                    for (var v = vals.First();v!=null;v=v.Next())
                        if (v.value().nominalDataType.Compare(w, v.value().Eval(tr,cx)) == 0)
                            return TBool.For(found);
                    return TBool.For(!found);
                }
                else
                {
                    for (var rb = where.RowSets(tr,cx).First(cx); rb != null; rb = rb.Next(cx))
                    {
                        if (w.dataType.Length == 0)
                        {
                            var col = rb.row[0];
                            var dt = col.dataType;
                            if (dt.Compare(w, col) == 0)
                                return TBool.For(found);
                        }
                        else if (w.dataType.Compare(w, rb.row) == 0)
                            return TBool.For(found);
                    }
                    return TBool.For(!found);
                }
            }
            return null;
        }
        internal override bool aggregates()
        {
            for (var v = vals?.First(); v != null; v = v.Next())
                if (v.value().aggregates())
                    return true;
            return what.aggregates() || base.aggregates();
        }
        internal override void Build(Context _cx,RowSet rs)
        {
            for (var v = vals?.First(); v != null; v = v.Next())
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
            for (var v = vals?.First(); v != null; v = v.Next())
                v.value()._AddIn(_cx,rb, ref aggsDone);
            what._AddIn(_cx,rb, ref aggsDone);
            base._AddIn(_cx,rb, ref aggsDone);
        }
        internal override SqlValue SetReg(RowBookmark rb)
        {
            bool nulls = true;
            for (var v = vals?.First(); v != null; v = v.Next())
                if (v.value().SetReg(rb) != null)
                    nulls = false;
            return (what.SetReg(rb) == null && nulls) ? null : this;
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
        internal readonly static long
            Found = -382, // bool
            Lhs = -383, // SqlValue
            Rhs = -384; // SqlValue
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
            : base(dp, new BTree<long,object>(NominalType,Domain.Bool)
                  +(Lhs,a)+(Found,f)+(Rhs,b)+(Depth,1+_Max(a.depth,b.depth))
                  +(Dependents,BList<long>.Empty+a.defpos+b.defpos))
        { }
        protected MemberPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static MemberPredicate operator+(MemberPredicate s,(long,object)x)
        {
            return new MemberPredicate(s.defpos, s.mem+x);
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (MemberPredicate)base.Replace(cx, so, sv);
            var lf = r.left.Replace(cx, so, sv);
            if (lf != r.left)
                r += (Lhs,lf);
            var rg = r.rhs.Replace(cx, so, sv);
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
        internal override Query Conditions(Transaction tr, Context cx, Query q,bool disj,out bool move)
        {
            move = false;
            q = lhs.Conditions(tr, cx, q, false,out _);
            q = rhs.Conditions(tr, cx, q, false, out _);
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
                    return nominalDataType.defaultValue;
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
        internal override SqlValue SetReg(RowBookmark rb)
        {
            return (lhs.SetReg(rb) != null || rhs.SetReg(rb) != null) ? this : null;
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
        public SqlValue lhs => (SqlValue)mem[MemberPredicate.Lhs];
        /// <summary>
        /// OF or NOT OF
        /// </summary>
        public bool found => (bool)(mem[MemberPredicate.Found]??false);
        /// <summary>
        /// the right operand
        /// </summary>
        public Domain[] rhs => (Domain[])mem[MemberPredicate.Rhs]; // naughty: MemberPreciate Rhs is SqlValue
        /// <summary>
        /// Constructor: a member predicate from the parser
        /// </summary>
        /// <param name="a">the left operand</param>
        /// <param name="f">found or not found</param>
        /// <param name="b">the right operand</param>
        internal TypePredicate(long dp,SqlValue a, bool f, Domain[] r)
            : base(dp, new BTree<long,object>(NominalType,Domain.Bool)
                  +(MemberPredicate.Lhs,a)+(MemberPredicate.Found,f)
                  +(MemberPredicate.Rhs,r))
        {  }
        protected TypePredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static TypePredicate operator+(TypePredicate s,(long,object)x)
        {
            return new TypePredicate(s.defpos, s.mem + x);
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
        internal override Query Conditions(Transaction tr, Context cx, Query q, bool disj, out bool move)
        {
            move = false;
            q = lhs.Conditions(tr, cx, q, false, out _);
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
            foreach (var tt in rhs)
                b = at.EqualOrStrongSubtypeOf(tt); // implemented as Equals for ONLY
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
            foreach (var tt in rhs)
                b = at.EqualOrStrongSubtypeOf(tt); // implemented as Equals for ONLY
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
            :base(dp,BTree<long,object>.Empty+(NominalType,Domain.Bool)
                 +(Left,op1)+(Right,op2)+(SqlValueExpr.Kind,op1)
                 +(Dependents,BList<long>.Empty+op1.defpos+op2.defpos)
                 +(Depth,1+_Max(op1.depth,op2.depth)))
        { }
        protected PeriodPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static PeriodPredicate operator+(PeriodPredicate s,(long,object)x)
        {
            return new PeriodPredicate(s.defpos, s.mem + x);
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (PeriodPredicate)base.Replace(cx, so, sv);
            var a = r.left.Replace(cx, so, sv);
            if (a != r.left)
                r += (Left,a);
            var b = r.right.Replace(cx, so, sv);
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
        internal override SqlValue SetReg(RowBookmark rb)
        {
            return (left?.SetReg(rb) != null || right?.SetReg(rb) != null) ? this : null;
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
        internal readonly static long
            QExpr = -385; // Query
        public Query expr => (Query)mem[QExpr];
        /// <summary>
        /// the base query
        /// </summary>
        public QueryPredicate(long dp,Query e,BTree<long,object>m=null) 
            : base(dp, (m??BTree<long,object>.Empty)+(NominalType,Domain.Bool)+(QExpr,e)
                  +(Dependents,new BList<long>(e.defpos))+(Depth,1+e.depth))
        {  }
        protected QueryPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        /// <summary>
        /// analysis stage Conditions: analyse the expr (up to building its rowset)
        /// </summary>
        internal override Query Conditions(Transaction tr, Context cx, Query q, bool disj, out bool move)
        {
            move = false;
            return expr;
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
        internal override SqlValue SetReg(RowBookmark rb)
        {
            expr.SetReg(rb);
            return base.SetReg(rb);
        }
    }
    /// <summary>
    /// the EXISTS predicate
    /// </summary>
    internal class ExistsPredicate : QueryPredicate
    {
        public ExistsPredicate(long dp,Query e) : base(dp,e,BTree<long,object>.Empty
            +(Dependents,new BList<long>(e.defpos))+(Depth,1+e.depth)) { }
        protected ExistsPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static ExistsPredicate operator+(ExistsPredicate s,(long,object)x)
        {
            return new ExistsPredicate(s.defpos, s.mem + x);
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
            return "EXISTS..";
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
        internal readonly static long
            NVal = -386, //SqlValue
            NIsNull = -387; //bool
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
            : base(dp,new BTree<long,object>(NominalType,Domain.Bool)
                  +(NVal,v)+(NIsNull,b)+(Dependents,BList<long>.Empty+v.defpos)
                  +(Depth,1+v.depth))
        { }
        protected NullPredicate(long dp, BTree<long, object> m) : base(dp, m) { }
        public static NullPredicate operator+(NullPredicate s,(long,object)x)
        {
            return new NullPredicate(s.defpos, s.mem + x);
        }

        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (NullPredicate)base.Replace(cx, so, sv);
            var vl = r.val.Replace(cx, so, sv);
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
        internal override SqlValue SetReg(RowBookmark rb)
        {
            return (val.SetReg(rb) != null) ? this : null;
        }
        public override string ToString()
        {
            return isnull?"IS NULL":"IS NOT NULL";
        }
    }
    internal abstract class SqlHttpBase : SqlValue
    {
        internal const long
            GlobalFrom = -366, // Query
            HttpWhere = -367, // BTree<long,SqlValue>
            HttpMatches = -368, // BTree<SqlValue,TypedValue>
            HttpRows = -369; // RowSet
        internal Query globalFrom => (Query)mem[GlobalFrom];
        public BTree<long,SqlValue> where => 
            (BTree<long,SqlValue>)mem[HttpWhere]??BTree<long,SqlValue>.Empty;
        public BTree<SqlValue, TypedValue> matches=>
            (BTree<SqlValue,TypedValue>)mem[HttpMatches]??BTree<SqlValue,TypedValue>.Empty;
        protected RowSet rows => (RowSet)mem[HttpRows]??EmptyRowSet.Value;
        protected SqlHttpBase(long dp, Query q,BTree<long,object> m=null) : base(dp, 
            (m??BTree<long,object>.Empty)+(NominalType,q.rowType)+(HttpMatches,q.matches)
            +(GlobalFrom,q))
        { }
        protected SqlHttpBase(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlHttpBase operator+(SqlHttpBase s,(long,object)x)
        {
            return (SqlHttpBase)s.New(s.mem + x);
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlHttpBase)base.Replace(cx, so, sv);
            var gf = r.globalFrom.Replace(cx, so, sv);
            if (gf != r.globalFrom)
                r += (GlobalFrom, gf);
            var wh = r.where;
            for (var b=wh.First();b!=null;b=b.Next())
            {
                var v = b.value().Replace(cx, so, sv);
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
        internal virtual SqlHttpBase AddCondition(SqlValue wh)
        {
            return (wh!=null)? this+(HttpWhere,where+(wh.defpos, wh)):this;
        }
        internal void AddCondition(BTree<long,SqlValue> svs)
        {
            for (var b = svs.First(); b != null; b = b.Next())
                AddCondition(b.value());
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
        internal readonly static long
            KeyType = -388, // Domain
            Mime = -389, // string
            Pre = -390, // TRow
            RemoteCols = -391, //string
            TargetType = -392, //Domain
            Url = -393; //SqlValue
        public SqlValue expr => (SqlValue)mem[Url]; // for the url
        public string mime=>(string)mem[Mime];
        public TRow pre => (TRow)mem[Pre];
        public Domain targetType=> (Domain)mem[TargetType];
        public Domain keyType => (Domain)mem[KeyType];
        public string remoteCols => (string)mem[RemoteCols];
        internal SqlHttp(long dp, Query gf, SqlValue v, string m, Domain tgt,
            BTree<long, SqlValue> w, string rCols, TRow ur = null, BTree<SqlValue, TypedValue> mts = null)
            : base(dp,gf,BTree<long,object>.Empty+(HttpWhere,w)+(HttpMatches,mts)
                  +(Url,v)+(Mime,m)+(Pre,ur)+(TargetType,tgt)+(RemoteCols,rCols))
        { }
        protected SqlHttp(long dp, BTree<long, object> m) : base(dp, m) { }
        public static SqlHttp operator+(SqlHttp s,(long,object)x)
        {
            return new SqlHttp(s.defpos, s.mem + x);
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = (SqlHttp)base.Replace(cx, so, sv);
            var u = r.expr.Replace(cx, so, sv);
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
                    OnEval(tr,ev):null;
            if ((!asArray) && r is TArray ta)
                return new TRowSet(tr,cx,globalFrom, globalFrom.rowType,ta.ToArray());
            if (r != null)
                return r;
            var rtype = globalFrom.source.rowType;
            return rtype.defaultValue;
        }
        TypedValue OnEval(Transaction tr, TypedValue ev)
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
                var rq = GetRequest(tr, url);
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
                            Grouped(tr, gs.value(), sql, ref cm, ids, globalFrom);
                    for (var b = globalFrom.rowType.names.First(); b != null; b = b.Next())
                        if (!ids.Contains(b.key()))
                        {
                            ids.Add(b.key());
                            sql.Append(cm); cm = ",";
                            sql.Append(b.key());
                        }
                    var keycols = BList<SqlValue>.Empty;
                    foreach (var id in ids)
                        keycols+=globalFrom.cols[globalFrom.rowType.names[id].seq];
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
                    r = rtype.Parse(new StreamReader(s).ReadToEnd());
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
        void Grouped(Transaction tr,Grouping gs,StringBuilder sql,ref string cm,List<string> ids,Query gf)
        {
            for (var b = gs.members.First(); b!=null;b=b.Next())
            {
                var g = b.key();
                if (gf.rowType.names.Contains(g.name) && !ids.Contains(g.name))
                {
                    ids.Add(g.name);
                    sql.Append(cm); cm = ",";
                    sql.Append(g.name);
                }
            }
            for (var gi = gs.groups.First();gi!=null;gi=gi.Next())
                Grouped(tr,gi.value(), sql, ref cm,ids, gf);
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
    /// To implement RESTViews properly we need to hack the nominalDataType of the FROM globalView.
    /// After stage Selects, globalFrom.nominalDataType is as declared in the view definition.
    /// So globalRowSet always has the same rowType as globalfrom,
    /// and the same grouping operation takes place on each remote contributor
    /// </summary>
    internal class SqlHttpUsing : SqlHttpBase
    {
        internal const long
            UsingCols = -370, // BTree<string,long>
            UsingTablePos = -371; // long
        internal long usingtablepos => (long)(mem[usingtablepos] ?? 0);
        internal BTree<string, long> usC =>
            (BTree<string,long>)mem[UsingCols]??BTree<string, long>.Empty;
        // the globalRowSetType is our nominalDataType
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
        internal override SqlValue _Setup(Transaction tr,Context cx,Query q,Domain d)
        {
            return this+(NominalType,d); // this value is overwritten later
        }
        internal override SqlHttpBase AddCondition(SqlValue wh)
        {
            return base.AddCondition(wh?.PartsIn(globalFrom.source.rowType));
        }
        internal override TypedValue Eval(Transaction tr, Context cx, bool asArray)
        {
            var qs = globalFrom.QuerySpec(); // can be a From if we are in a join
            var cs = globalFrom.source as CursorSpecification;
            cs.MoveConditions(cx,cs.usingFrom); // probably updates all the queries
            qs = globalFrom.QuerySpec();
            cs = globalFrom.source as CursorSpecification;
            var uf = cs.usingFrom as Table;
            var usingTable = tr.role.objects[usingtablepos] as Table;
            var usingIndex = usingTable.FindPrimaryIndex();
            var usingTableColumns = usingTable.rowType.columns;
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
                var s = new SqlHttp(tr, f, new SqlLiteral(tr, url), "application/json", f.source.nominalDataType, Query.PartsIn(where,f.source.nominalDataType),"",ur);
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
                var s = new SqlHttp(tr, f, new SqlLiteral(tr, url), "application/json", f.source.nominalDataType, Query.PartsIn(f.source.where, f.source.nominalDataType), "", ur);
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
