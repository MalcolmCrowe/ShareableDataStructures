using System;
using System.Globalization;
using System.Text;
using System.Xml;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
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
    /// In this work a Domain includes details of columns of structured types. 
    /// Parsing results in construction of many "ad-hoc" domains with heap uids or -1L. 
    /// When database objects are committed, the Domain.Create method checks for a similar data type
    /// already in the database. The Domain.CompareTo method supports this comparison.
    /// Column uids in SqlValues and RowSets are SqlValues that include
    /// evaluation instructions: Domain.rowType comparsion checks for matching columns
    /// at this level (not just comparison of the column domains).
    /// Thus the "dataType" field of an SqlValue in general determines how to evaluate
    /// the value, not just the underlying primitive types.
    /// Immutable (everything in level 3 must be immutable)
    ///     // shareable as of 26 April 2021 (depends on TypedValue remaining shareable)
    /// </summary>
    internal class Domain : DBObject, IComparable
    {
        // Annoyingly, I can't seem to make these definitions immutable
        internal static Domain Null, Value, Content, // Pyrrho 5.1 default type for Document entries, from 6.2 for generic scalar value
    Bool, Blob, Char, Password, XML, Int, Numeric, Real, Date, Timespan, Timestamp,
    Interval, TypeSpec, _Level, Physical, MTree, // pseudo type for MTree implementation
    Partial, // pseudo type for MTree implementation
    Array, Multiset, Collection, Cursor, UnionNumeric, UnionDate,
    UnionDateNumeric, Exception, Period,
    Document, DocArray, ObjectId, JavaScript, ArgList, // Pyrrho 5.1
    TableType, Row, Delta, Position, Role,
    Metadata, HttpDate, Star, // Pyrrho v7
    Rvv; // Rvv is V7 validator type
        /// <summary>
        /// A new system Union type
        /// </summary>
        /// <param name="lp"></param>
        /// <param name="ut"></param>
        /// <returns></returns>
        internal static Domain UnionType(long lp, params Domain[] ut)
        {
            var u = CTree<Domain, bool>.Empty;
            foreach (var d in ut)
                u += (d, true);
            return new Domain(lp, Sqlx.UNION, u);
        }
        // Uids used in Domains, Tables, TableColumns, Functions to define mem.
        // named mem will have positive uids.
        // indexes are handled separately.
        // Usage: D=Domain,C=TableColumn,T=Table,F=Function,E=Expression
        internal const long
            Abbreviation = -70, // string (D)
            Aggs = -191, // CTree<long,bool> SqlValue
            Charset = -71, // Pyrrho.Common.CharSet (C E)
            Constraints = -72,  // CTree<long,bool> DBObjects 
            Culture = -73, // System.Globalization.CultureInfo (C E)
            Default = -74, // TypedValue (D C)
            DefaultString = -75, // string
            DefaultRowValues = -216, // CTree<long,TypedValue>
            Descending = -76, // Sqlx
            Display = -177, // int
            Element = -77, // Domain
            End = -78, // Sqlx (interval part) (D)
            Iri = -79, // string
            Kind = -80, // Sqlx
            NotNull = -81, // bool
            NullsFirst = -82, // bool (C)
            OrderCategory = -83, // OrderCategory
            OrderFunc = -84, // long DBObject
            Precision = -85, // int (D)
            Provenance = -86, // string (D)
            Representation = -87, // CTree<long,Domain> DBObjects,Domain
            RowType = -187,  // CList<long> 
            Scale = -88, // int (D)
            Start = -89, // Sqlx (D)
            Structure = -391, // long Table 
            UnionOf = -91; // CTree<Domain,bool>
        internal static void StandardTypes()
        {
            Null = new StandardDataType(Sqlx.Null);
            Value = new StandardDataType(Sqlx.VALUE); // Not known whether scalar or row type
            Content = new StandardDataType(Sqlx.CONTENT); // Pyrrho 5.1 default type for Document entries, from 6.2 for generic scalar value
            Bool = new StandardDataType(Sqlx.BOOLEAN);
            Blob = new StandardDataType(Sqlx.BLOB);
            Char = new StandardDataType(Sqlx.CHAR, Common.OrderCategory.Primitive);
            Password = new StandardDataType(Sqlx.PASSWORD, Common.OrderCategory.Primitive);
            XML = new StandardDataType(Sqlx.XML);
            Int = new StandardDataType(Sqlx.INTEGER, Common.OrderCategory.Primitive);
            Numeric = new StandardDataType(Sqlx.NUMERIC, Common.OrderCategory.Primitive);
            Real = new StandardDataType(Sqlx.REAL, Common.OrderCategory.Primitive);
            Date = new StandardDataType(Sqlx.DATE, Common.OrderCategory.Primitive);
            HttpDate = new StandardDataType(Sqlx.HTTPDATE, Common.OrderCategory.Primitive);
            Timespan = new StandardDataType(Sqlx.TIME, Common.OrderCategory.Primitive);
            Timestamp = new StandardDataType(Sqlx.TIMESTAMP, Common.OrderCategory.Primitive);
            Interval = new StandardDataType(Sqlx.INTERVAL, Common.OrderCategory.Primitive);
            TypeSpec = new StandardDataType(Sqlx.TYPE);
            _Level = new StandardDataType(Sqlx.LEVEL);
            Physical = new StandardDataType(Sqlx.DATA);
            MTree = new StandardDataType(Sqlx.M); // pseudo type for MTree implementation
            Partial = new StandardDataType(Sqlx.T); // pseudo type for MTree implementation
            Array = new StandardDataType(Sqlx.ARRAY, Common.OrderCategory.None,Content);
            Multiset = new StandardDataType(Sqlx.MULTISET, Common.OrderCategory.None, Content);
            Collection = UnionType(--_uid, Array, Multiset);
            Cursor = new StandardDataType(Sqlx.CURSOR);
            UnionNumeric = UnionType(--_uid, Int, Numeric, Real);
            UnionDate = UnionType(--_uid, Date, Timespan, Timestamp, Interval);
            UnionDateNumeric = UnionType(--_uid, Date, Timespan, Timestamp, Interval, Int, Numeric, Real);
            Exception = new StandardDataType(Sqlx.HANDLER);
            Period = new StandardDataType(Sqlx.PERIOD);
            Document = new StandardDataType(Sqlx.DOCUMENT); // Pyrrho 5.1
            DocArray = new StandardDataType(Sqlx.DOCARRAY); // Pyrrho 5.1
            ObjectId = new StandardDataType(Sqlx.OBJECT); // Pyrrho 5.1
            JavaScript = new StandardDataType(Sqlx.ROUTINE); // Pyrrho 5.1
            ArgList = new StandardDataType(Sqlx.CALL); // Pyrrho 5.1
            TableType = new StandardDataType(Sqlx.TABLE);
            Row = new StandardDataType(Sqlx.ROW);
            Delta = new StandardDataType(Sqlx.INCREMENT);
            Role = new StandardDataType(Sqlx.ROLE);
            Rvv = new StandardDataType(Sqlx.CHECK);
            Position = new StandardDataType(Sqlx.POSITION);
            Metadata = new StandardDataType(Sqlx.METADATA);
            Star = new Domain(--_uid, Sqlx.TIMES, BTree<long, object>.Empty);
        }
        public Sqlx kind => (Sqlx)(mem[Kind] ?? Sqlx.NO);
        public int prec => (int)(mem[Precision] ?? 0);
        public int scale => (int)(mem[Scale] ?? 0);
        public Sqlx start => (Sqlx)(mem[Start] ?? Sqlx.NULL);
        public Sqlx end => (Sqlx)(mem[End] ?? Sqlx.NULL);
        public Sqlx AscDesc => (Sqlx)(mem[Descending] ?? Sqlx.ASC);
        public bool notNull => (bool)(mem[NotNull] ?? false);
        public Sqlx nulls => (Sqlx)(mem[NullsFirst] ?? Sqlx.NULL);
        public CharSet charSet => (CharSet)(mem[Charset] ?? CharSet.UCS);
        public CultureInfo culture => (CultureInfo)(mem[Culture] ?? CultureInfo.InvariantCulture);
        public Domain elType => (Domain)mem[Element]??Domain.Null;
        public TypedValue defaultValue => (TypedValue)mem[Default] ??
            ((defaultRowValues!=CTree<long,TypedValue>.Empty)? new TRow(this,defaultRowValues) 
                        : TNull.Value.New(this));
        public string defaultString => (string)mem[DefaultString] ?? "";
        public CTree<long, TypedValue> defaultRowValues =>
            (CTree<long, TypedValue>)mem[DefaultRowValues] ?? CTree<long, TypedValue>.Empty;
        public int display => (int)(mem[Display] ?? rowType.Length);
        public string abbrev => (string)mem[Abbreviation]??"";
        public CTree<long, bool> constraints => (CTree<long, bool>)mem[Constraints]??CTree<long,bool>.Empty;
        public string iri => (string)mem[Iri];
        public string provenance => (string)mem[Provenance];
        public long structure => (long)(mem[Structure] ?? -1L);
        public CTree<long,Domain> representation => 
            (CTree<long,Domain>)mem[Representation] ?? CTree<long,Domain>.Empty;
        public virtual CList<long> rowType => (CList<long>)mem[RowType] ?? CList<long>.Empty;
        public int Length => rowType.Length;
        public Procedure orderFunc => (Procedure)mem[OrderFunc];
        public CTree<long,bool> aggs => 
            (CTree<long,bool>)mem[Aggs]??CTree<long,bool>.Empty;
        public OrderCategory orderflags => (OrderCategory)(mem[OrderCategory]??Common.OrderCategory.None);
        public CTree<Domain,bool> unionOf => 
            (CTree<Domain,bool>)mem[UnionOf] ?? CTree<Domain,bool>.Empty;
        internal Domain(long dp,Context cx,Sqlx t, CTree<long, Domain> rs, CList<long> rt, int ds=0)
            : this(dp,_Mem(cx,t,rs,rt,ds) 
                  + (Representation, rs) + (RowType, rt) + (_Depth,cx.Depth(rs))) 
        {
            cx.Add(this);
        }
        public Domain(Sqlx t, Context cx, BList<SqlValue> vs, int ds=0)
            : this(cx.GetUid(),_Mem(cx,t,vs,ds)) 
        {
            cx.Add(this);
        }
        public Domain(long dp,Context cx,Sqlx t, BList<SqlValue> vs, int ds = 0)
    : this(dp, _Mem(cx,t,vs, ds))
        {
            cx.Add(this);
        }
        public Domain(Sqlx t,Context cx, CList<long> cs, int ds=0)
            : this(cx.GetUid(),_Mem(cx,t,cs,ds)) 
        {
            cx.Add(this);
        }
        // A union of standard types
        public Domain(long dp, Sqlx t, CTree<Domain,bool> u)
            : this(dp,BTree<long,object>.Empty + (Kind,t) + (UnionOf,u))
        {
            Database._system += (dp, this,0);
        }
        // A simple standard type
        public Domain(long dp, Sqlx t, BTree<long,object> u)
        : base(dp,_Mem(t,u)  + (Descending,Sqlx.ASC))
        {
            Database._system += (dp, this, 0);
        }
        public Domain(Context cx,Domain d) :this(cx.GetUid(),d.mem)
        {
            cx.Add(this);
        }
        protected Domain(long dp, BTree<long, object> m) : base(dp, _Mem(m))
        { }
        /// <summary>
        /// Allow construction of ad-hoc derived types such as ARRAY, MULTISET
        /// </summary>
        /// <param name="t"></param>
        /// <param name="d"></param>
        public Domain(long dp,Sqlx t, Domain et)
            : base(dp,new BTree<long, object>(Element, et)+(Kind,t))
        { }
        /// <summary>
        /// Constructor: a newly defined Domain
        /// </summary>
        /// <param name="p">The PDomain level 2 definition</param>
        public Domain(PDomain p) 
            : base(p.ppos,_Mem(p) + (LastChange, p.ppos))
        { }
        static BTree<long,object> _Mem(BTree<long,object> u)
        {
            if (u == null)
                u = BTree<long,object>.Empty;
            var d = (int)(u[_Depth] ?? 1);
            if (u.Contains(RowType) && d == 1)
                u += (_Depth, 2);
            if (!u.Contains(Default) && u.Contains(Kind))
                u += (Default, For((Sqlx)u[Kind]).defaultValue);
            return u;
        }
        static BTree<long, object> _Mem(Sqlx t,BTree<long, object> u)
        {
            if (u == null)
                u = BTree<long, object>.Empty;
            u += (Kind,t);
            var d = (int)(u[_Depth] ?? 1);
            if (u.Contains(RowType) && d == 1)
                u += (_Depth, 2);
            return u;
        }
        static BTree<long,object> _Mem(Context cx,Sqlx t,BList<SqlValue> vs,int ds=0)
        {
            var rs = CTree<long, Domain>.Empty;
            var cs = CList<long>.Empty;
            var dp = CTree<long, bool>.Empty;
            var ag = CTree<long, bool>.Empty;
            var de = 0;
            for (var b = vs.First(); b != null; b = b.Next())
            {
                var v = b.value();
                rs += (v.defpos, cx._Dom(v));
                cs += v.defpos;
                dp += (v.defpos, true);
                ag += v.IsAggregation(cx);
                de = Math.Max(de, v.depth);
            }
            var m = BTree<long, object>.Empty + (Representation, rs) + (RowType, cs)
                + (Aggs,ag) + (_Depth,de+1) + (Dependents,dp) + (Kind,t)
                + (Default, For(t).defaultValue);
            if (ds != 0)
                m += (Display, ds);
            return m + (_Depth,cx.Depth(vs));
        }
        static BTree<long, object> _Mem(Context cx,Sqlx t,BTree<long,Domain> rs, CList<long> cs, int ds = 0)
        {
            for (var b = cs.First(); b != null; b = b.Next())
                if (!rs.Contains(b.value()))
                    throw new PEException("PE283");
            var m = new BTree<long, object>(RowType, cs) + (Kind, t)
                + (Default, For(t).defaultValue);
            if (ds != 0)
                m += (Display, ds);
            return m + (_Depth,cx.Depth(cs));
        }

        internal Domain Best(Domain dt)
        {
            if (EqualOrStrongSubtypeOf(dt))
                return this;
            if (kind == Sqlx.UNION)
                for (var b = unionOf.First(); b != null; b = b.Next())
                    if (b.key() is Domain && EqualOrStrongSubtypeOf(b.key()))
                        return this;
            return dt;
        }

        static BTree<long, object> _Mem(Context cx, Sqlx t,CList<long> cs, int ds = 0)
        {
            var rs = CTree<long, Domain>.Empty;
            for (var b = cs.First(); b != null; b = b.Next())
            {
                var p = b.value();
                rs += (p, cx._Dom(cx.obs[p]??(DBObject)cx.db.objects[p]));
            }
            var m = BTree<long, object>.Empty + (Representation, rs) + (RowType, cs)
                + (Kind, t) + (Default, For(t).defaultValue);
            if (ds != 0)
                m += (Display, ds);
            return m + (_Depth, cx.Depth(cs));
        }
        static BTree<long,object> _Mem(PDomain pd)
        {
            var m = pd.domain.mem;
            if (pd.domain.kind == Sqlx.TYPE)
                m += (Structure, pd.eldefpos);
            if (!m.Contains(Default))
                m += (Default, For(pd.domain.kind).defaultValue);
            return m;
        }
        internal override DBObject Instance(long lp,Context cx, Domain q,BList<Ident>cs=null)
        {
            var r = base.Instance(lp,cx, q);
            for (var b = constraints.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is DBObject ob)
                    cx.Add(ob.Instance(lp,cx));
            return r;
        }
        public static Domain operator+(Domain d,(long,object)x)
        {
            var (k, v) = x;
            if (v!=null && !(v is IComparable))
                throw new PEException("PE900");
            return (Domain)d.New(d.mem + x);
        }
        public static Domain operator-(Domain d,long x)
        {
            var rp = CTree<long, Domain>.Empty;
            var m = d.mem;
            var ch = false;
            var ds = d.display;
            for (var b = d.representation.First(); b != null; b = b.Next())
            {
                var p = b.key();
                if (p == x)
                    ch = true;
                else
                    rp += (b.key(), b.value());
            }
            if (ch)
                m += (Representation, rp);
            var rt = CList<long>.Empty;
            for (var b=d.rowType.First();b!=null;b=b.Next())
            {
                var p = b.value();
                if (p == x)
                {
                    ch = true;
                    if (b.key() < ds)
                        ds--;
                }
                else
                    rt += p;
            }
            if (ch)
                m = m + (RowType, rt) + (Display,ds);
            return (Domain)d.New(m);
        }
        public static Domain operator+ (Domain d,(Context,SqlValue) x)
        {
            var (cx, sv) = x;
            var m = d.mem + (RowType, d.rowType + sv.defpos)
                + (Representation, d.representation + (sv.defpos, cx._Dom(sv)));
            m += (Aggs, d.aggs + sv.IsAggregation(cx));
            return (Domain)d.New(m);
        }
        public static Domain operator+(Domain d,(Sqlx,Sqlx)o)
        {
            if (d.AscDesc != o.Item1)
                d += (Descending, o.Item1);
            if (d.nulls != o.Item2)
                d += (NullsFirst, o.Item2);
            return d;
        }
        internal bool IsStar(Context cx)
        {
            return rowType.Length == 1 && cx.obs[rowType[0]] is SqlStar;
        }
        public long this[int i] => (i>=0 && i<rowType.Length)?rowType[i]:-1L;
        public ABookmark<int,long> First()
        {
            return rowType.First();
        }
        /// <summary>
        /// A feature of SQL is that up to the point of Commit, the domain
        /// inheritance tree is acyclic. This little routine in
        /// combination with Physical.Dependents(), ensures that
        /// referenced domains are guaranteed to be in the committed database.
        /// </summary>
        /// <param name="wr"></param>
        /// <param name="tr"></param>
        /// <returns></returns>
        internal long Create(Writer wr,Transaction tr)
        {
            if (defpos >= 0)
                return defpos;
            if (wr.cx.db.types.Contains(this))
                return wr.cx.db.types[this];
            Physical pp = new PDomain(this,wr.Length,wr.cx);
            (_,pp) = pp.Commit(wr, tr);
            // Assert(CompareTo(pp.domaim)==0) and tr unchanged
            return pp.ppos;
        }
        internal CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> s)
        {
            for (var b = rowType?.First(); b != null; b = b.Next())
                if (cx.obs[b.value()] is SqlValue sv)
                    s = sv.Needs(cx, r, s);
            return s;
        }
        internal Domain Resolve(Context cx)
        {
            var rs = representation;
            for (var b = rs.First(); b != null; b = b.Next())
                if ((b.value() == Content || b.value().kind == Sqlx.UNION)
                    && cx.obs[b.key()] is SqlValue sv
                    && sv.domain != Content.defpos)
                        rs += (sv.defpos, sv._Dom(cx));
            return (rs==representation)?this:this+(Representation,rs);
        }
        internal bool HasStar(Context cx, RowSet f = null)
        {
            for (var b = rowType.First(); b != null; b = b.Next())
            {
                if (cx.obs[b.value()] is SqlStar st
                    && (f == null || (st.prefix < 0
                    || (cx.obs[st.prefix] is SqlValue sv
                        && cx.defs[sv.name][cx.iim[sv.defpos].sd].Item1.dp == f.defpos))))
                    return true;
            }
            return false;
        }
        internal override Database Add(Database d, PMetadata pm, long p)
        {
            d = base.Add(d, pm, p);
            if (pm.iri!="")
                d += (this + (Iri, pm.iri), p);
            return d;
        }
        internal void Show(StringBuilder sb)
        {
            if (defpos >= 0)
            {
                sb.Append(" "); sb.Append(Uid(defpos));
            }
            if (name != "")
            {
                sb.Append(" "); sb.Append(name);
            }
            sb.Append(' '); sb.Append(kind);
            if (mem.Contains(RowType))
            {
                var cm = " (";
                for (var b = rowType.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = (b.key() + 1 == display) ? "|" : ",";
                    sb.Append(Uid(b.value()));
                    sb.Append(' ');
                    if (representation[b.value()] is Domain dm)
                    {
                        if (dm.name != "")
                            sb.Append(dm.name);
                        else 
                            sb.Append(dm.kind);
                    }
                }
                sb.Append(")");
            }
            if (mem.Contains(Aggs))
            {
                var cm = " Aggs (";
                for (var b = aggs.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                sb.Append(")");
            }
            _Show(sb);
        }
        internal void _Show(StringBuilder sb)
        {
            if (mem.Contains(Display)) { sb.Append(" Display="); sb.Append(display); }
            if (mem.Contains(Abbreviation)) { sb.Append(' '); sb.Append(abbrev); }
            if (mem.Contains(Charset) && charSet != CharSet.UCS)
            { sb.Append(" CharSet="); sb.Append(charSet); }
            if (mem.Contains(Culture) && culture.Name != "")
            { sb.Append(" Culture="); sb.Append(culture.Name); }
            if (!defaultValue.IsNull)
            { sb.Append(" Default="); sb.Append(defaultValue); }
            if (mem.Contains(Element))
            { sb.Append(" elType="); sb.Append(elType); }
            if (mem.Contains(End)) { sb.Append(" End="); sb.Append(end); }
            // if (mem.Contains(Names)) { sb.Append(' '); sb.Append(names); } done in Columns
            if (mem.Contains(OrderCategory) && orderflags != Common.OrderCategory.None
                && orderflags!=Common.OrderCategory.Primitive)
            { sb.Append(' '); sb.Append(orderflags); }
            if (mem.Contains(OrderFunc)) { sb.Append(" OrderFunc="); sb.Append(orderFunc); }
            if (mem.Contains(Precision) && prec != 0) { sb.Append(" Prec="); sb.Append(prec); }
            if (mem.Contains(Provenance)) { sb.Append(" Provenance="); sb.Append(provenance); }
            if (mem.Contains(Scale) && scale != 0) { sb.Append(" Scale="); sb.Append(scale); }
            if (mem.Contains(Start)) { sb.Append(" Start="); sb.Append(start); }
            if (AscDesc == Sqlx.DESC) sb.Append(" DESC");
            if (nulls != Sqlx.NULL) sb.Append(" " + nulls);
        }
        /// <summary>
        /// A readable version of the Domain
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder(GetType().Name);
            if (name != "")
            {
                sb.Append(" "); sb.Append(name);
            }
            sb.Append(' ');sb.Append(kind);
            if (mem.Contains(RowType))
            {
                var cm = " (";
                for (var b=rowType.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm=(b.key()+1==display)?"|":",";
                    sb.Append(Uid(b.value()));
                }
                if (rowType != CList<long>.Empty)
                    sb.Append(")");
            }
            _Show(sb);
            if (mem.Contains(Representation)) {
                var cm = "";
                if (mem.Contains(RowType) && rowType.Length==representation.Count)
                    for (var b=rowType.First();b!=null;b=b.Next())
                    {
                        sb.Append(cm); cm = ",";
                        sb.Append("[" + Uid(b.value()) + "," + representation[b.value()] + "]");
                    }
                else for (var b = representation.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append("[" + Uid(b.key()) + "," + b.value() + "]");
                }
            }
            if (mem.Contains(Aggs))
            {
                var cm = " Aggs (";
                for (var b = aggs.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                if (cm==",")
                    sb.Append(")");
            }
            if (mem.Contains(Structure))
            { sb.Append(" structure="); sb.Append(Uid(structure)); }
            if (mem.Contains(Constraints))
            { 
                var cm = " constraints=("; 
                for (var b=constraints.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                if (cm == ",") sb.Append(")");
            }
            return sb.ToString();
        }
        internal static Sqlx Equivalent(Sqlx kind)
        {
            switch (kind)
            {
                case Sqlx.NCHAR:
                case Sqlx.CLOB:
                case Sqlx.NCLOB:
                case Sqlx.VARCHAR: return Sqlx.CHAR;
                case Sqlx.POSITION:
                case Sqlx.INT:
                case Sqlx.BIGINT:
                case Sqlx.SMALLINT: return Sqlx.INTEGER;
                case Sqlx.DECIMAL:
                case Sqlx.DEC: return Sqlx.NUMERIC;
                case Sqlx.DOUBLE:
                case Sqlx.FLOAT: return Sqlx.REAL;
                //        case Sqlx.TABLE: return Sqlx.ROW; not equivalent!
                default:
                    return kind;
            }
        }
        internal bool CanBeAssigned(object o)
        {
            if (o == null)
                return true;
            if (o is TypedValue v)
                return v.dataType.EqualOrStrongSubtypeOf(this)
                    || CanBeAssigned(v.Val());
            switch (Equivalent(kind))
            {
                case Sqlx.PASSWORD:
                case Sqlx.CHAR: 
                    if (o is string)
                        return prec==0 || prec>=((string)o).Length;
                    return o is char;
                case Sqlx.NUMERIC:
                    if (o is Numeric) return true;
                    goto case Sqlx.INTEGER;
                case Sqlx.INTEGER: return o is int || o is long || o is Integer;
                case Sqlx.REAL:
                    if (o is double) return true;
                    goto case Sqlx.NUMERIC;
                case Sqlx.UNION:
                    for (var b = unionOf.First(); b != null; b = b.Next())
                        if (b.key() is Domain d && d.CanBeAssigned(o))
                            return true;
                    return false;
                case Sqlx.Null:
                    return false;
                case Sqlx.VALUE:
                case Sqlx.CONTENT:
                    return true;
                case Sqlx.BOOLEAN:
                    return o is bool;
            }
            return false;
        }
        internal int Typecode()
        {
            switch (Equivalent(kind))
            {
                case Sqlx.NULL: return 0;
                case Sqlx.INTEGER: return 1;
                case Sqlx.NUMERIC: return 2;
                case Sqlx.REAL: return 8;
                case Sqlx.CHECK: 
                case Sqlx.LEVEL:
                case Sqlx.NCHAR: return 3;
                case Sqlx.CHAR: return 3;
                case Sqlx.TIMESTAMP: return 4;
                case Sqlx.DATE: return 13;
                case Sqlx.BLOB: return 5;
                case Sqlx.ROW: return 6;
                case Sqlx.ARRAY: return 7;
                case Sqlx.MULTISET: return 7;
                case Sqlx.TABLE: return 7;
                case Sqlx.TYPE: return 12;
                case Sqlx.BOOLEAN: return 9;
                case Sqlx.INTERVAL: return 10;
                case Sqlx.TIME: return 11;
                case Sqlx.XML: return 3;
                case Sqlx.PERIOD: return 7;
                case Sqlx.PASSWORD: return 3;
            }
            return 0;
        }
        public TypedValue Get(BTree<long,Physical.Type>log,Reader rdr,long pp)
        {
            if (sensitive)
                return new TSensitive(this, Get(log,rdr,pp));
            switch (Equivalent(kind))
            {
                case Sqlx.NULL: return TNull.Value;
                case Sqlx.Null: return null;
                case Sqlx.BLOB: return new TBlob(this, rdr.GetBytes());
                case Sqlx.BOOLEAN: return (rdr.ReadByte() == 1) ? TBool.True : TBool.False;
                case Sqlx.CHAR: return new TChar(this, rdr.GetString());
                case Sqlx.DOCUMENT:
                    {
                        var i = 0;
                        return new TDocument(rdr.GetBytes(), ref i);
                    }
                case Sqlx.DOCARRAY: goto case Sqlx.DOCUMENT;
                case Sqlx.INCREMENT:
                    {
                        var r = new Delta();
                        var n = rdr.GetInt();
                        for (int i = 0; i < n; i++)
                        {
                            var ix = rdr.GetInt();
                            var h = (Common.Delta.Verb)rdr.ReadByte();
                            var nm = rdr.GetString();
                            r=new Delta(r.details+new Delta.Action(ix, h, nm, Get(log,rdr,pp)));
                        }
                        return r;
                    }
                case Sqlx.PASSWORD: goto case Sqlx.CHAR;
                case Sqlx.XML: goto case Sqlx.CHAR;
                case Sqlx.INTEGER:
                    {
                        var o = rdr.GetInteger();
                        return new TInteger(this, (Integer)o);
                    }
                case Sqlx.NUMERIC: return new TNumeric(this, rdr.GetDecimal());
                case Sqlx.REAL0: // merge with REAL (an anomaly happened between v5.0 and 5.5)
                case Sqlx.REAL: return new TReal(this, rdr.GetDouble());
                case Sqlx.DATE: return new TDateTime(this, rdr.GetDateTime());
                case Sqlx.TIME: return new TTimeSpan(this, new TimeSpan(rdr.GetLong()));
                case Sqlx.TIMESTAMP: return new TDateTime(this, new DateTime(rdr.GetLong()));
                case Sqlx.INTERVAL0: return new TInterval(this, rdr.GetInterval0()); //attempt backward compatibility
                case Sqlx.INTERVAL: return new TInterval(this, rdr.GetInterval());
                case Sqlx.ARRAY:
                    {
                        var dp = rdr.GetLong();
                        var el = (Domain)(rdr as Reader)?.role.infos[dp]??Content;
                        var vs = BList<TypedValue>.Empty;
                        var n = rdr.GetInt();
                        for (int j = 0; j < n; j++)
                        {
                            var dt = el.GetDataType(rdr);
                            vs += dt.Get(log,rdr,pp);
                        }
                        return new TArray(el,vs);
                    }
                case Sqlx.MULTISET:
                    {
                        var dp = rdr.GetLong();
                        var el = (Domain)(rdr as Reader)?.role.infos[dp]??Content;
                        var m = new TMultiset(el);
                        var n = rdr.GetInt();
                        for (int j = 0; j < n; j++)
                        {
                            var dt = el.GetDataType(rdr);
                            m.Add(dt.Get(log,rdr,pp));
                        }
                        return m;
                    }
                case Sqlx.REF:
                case Sqlx.ROW:
                case Sqlx.TABLE:
                    {
                        var dp = rdr.GetLong();
                        var vs = CTree<long,TypedValue>.Empty;
                        var dt = CTree<long, Domain>.Empty;
                        var rt = CList<long>.Empty;
                        var n = rdr.GetInt();
                        for (var j = 0; j < n; j++)
                        {
                            var c = rdr.GetString();
                            var (cp,cdt) = rdr.GetDomain(dp, c, pp);
                            dt += (cp, cdt);
                            rt += cp;
                            vs += (cp,cdt.Get(log,rdr,pp));
                        }
                        return new TRow(new Domain(rdr.context.GetUid(),rdr.context,Sqlx.ROW,dt,rt), vs);
                    }
                case Sqlx.TYPE:
                    {
                        var dp = rdr.GetLong();
                        var ut = rdr.GetDomain(dp,pp).Item1;
                        ut.Instance(dp, rdr.context,null);
                        var r = CTree<long, TypedValue>.Empty;
                        var n = rdr.GetInt();
                        for (var j = 0; j < n; j++)
                        {
                            var c = rdr.GetString();
                            var (cp,cdt) = rdr.GetDomain(ut.defpos, c, pp);
                            r += (cp,cdt.Get(log,rdr,pp));
                        }
                        return new TRow(ut,r);
                    }
                case Sqlx.CONTENT:
                    return new TChar(rdr.GetString());
            }
            throw new DBException("3D000").ISO();
        }
        internal DBObject ObjFor(Context cx,string c)
        {
            for (var b = rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                if (p >= Transaction.TransPos && cx.obs[p] is SqlValue ob && ob.name == c)
                    return ob;
                if (cx.db.role.infos[p] is ObInfo oi && oi.name == c)
                    return (DBObject)cx.db.objects[p];
            }
            return null;
        }
        internal long ColFor(Context context, string c)
        {
            for (var b=rowType.First();b!=null;b=b.Next())
            {
                var p = b.value();
                if ((p >= Transaction.TransPos && context.obs[p] is SqlValue ob && ob.name == c)
                    || (context.db.role.infos[p] is ObInfo oi && oi.name==c))
                        return p;
            }
            return -1L;
        }
        internal int PosFor(Context context, string c)
        {
            for (var b = rowType.First(); b != null; b = b.Next())
            {
                var oi = (ObInfo)context.db.role.infos[b.value()];
                if (oi.name == c)
                    return b.key();
            }
            return -1;
        }
        public Domain GetDataType(Reader rdr)
        {
            var b = (DataType)rdr.ReadByte();
            if (b == DataType.Null)
                return null;
            if (b == DataType.DomainRef)
                return rdr.GetDomain(rdr.GetLong(),rdr.Position).Item1;
            switch (b)
            {
                case DataType.Null: return Null;
                case DataType.TimeStamp: return Timestamp;
                case DataType.Interval: return Interval;
                case DataType.Integer: return Int;
                case DataType.Numeric: return Numeric;
                case DataType.String: return Char;
                case DataType.Date: return Date;
                case DataType.TimeSpan: return Timespan;
                case DataType.Boolean: return Bool;
                case DataType.Blob: return Blob;
                case DataType.Row: return Row;
                case DataType.Multiset: return Multiset;
                case DataType.Array: return Array;
                case DataType.Password: return Password;
            }
            return this;
        }
        public static Domain For(Sqlx dt)
        {
            switch (Equivalent(dt))
            {
                case Sqlx.CHAR: return Char;
                case Sqlx.TIMESTAMP: return Timestamp;
                case Sqlx.INTERVAL: return Interval;
                case Sqlx.INT: return Int;
                case Sqlx.NUMERIC: return Numeric;
                case Sqlx.DATE: return Date;
                case Sqlx.TIME: return Timespan;
                case Sqlx.BOOLEAN: return Bool;
                case Sqlx.BLOB: return Blob;
                case Sqlx.ROW: return Row;
                case Sqlx.MULTISET: return Multiset;
                case Sqlx.ARRAY: return Array;
                case Sqlx.PASSWORD: return Password;
                case Sqlx.TABLE: return TableType;
                case Sqlx.TYPE: return TypeSpec;
                case Sqlx.POSITION: return Position;
            }
            return Null;
        }
        /// <summary>
        /// Test for when to record subtype information. We want to do this when a value of
        /// a subtype is recorded in a column of the parent type, and the subtype information is
        /// not obtainable from the value alone. E.g. extra semantic information
        /// </summary>
        /// <param name="dt">The target type to check</param>
        /// <returns>true if this is a strong subtype of dt</returns>
        public virtual bool EqualOrStrongSubtypeOf(Domain dt)
        {
            var ki = Equivalent(kind);
            if (defpos > 0 && dt.defpos < 0 && ki == dt.kind)
                return true;
            if (CompareTo(dt)==0) // the Equal case
                return true;
            // Now consider subtypes
            if (sensitive != dt.sensitive)
                return false;
            if (dt == null)
                return true;
            var dk = Equivalent(dt.kind);
            if (dk == Sqlx.CONTENT || dk == Sqlx.Null)
                return true;
            if (ki == Sqlx.UNION && dk == Sqlx.UNION)
            {
                for (var b = unionOf.First(); b != null; b = b.Next())
                    if (b.key() is Domain dm && !dt.unionOf.Contains(dm))
                        return false;
                return true;
            }
            if (dt.kind==Sqlx.UNION)
            {
                for (var b = dt.unionOf.First(); b != null; b = b.Next())
                    if (b.key() is Domain dm && EqualOrStrongSubtypeOf(dm))
                        return true;
                return false;
            }
            if ((ki != Sqlx.ROW && ki != dk) || (ki == Sqlx.ROW && dk != Sqlx.ROW) ||
                elType != dt.elType)
                return false;
            return (dt.prec == 0 || prec == dt.prec) && (dt.scale == 0 || scale == dt.scale) &&
                (dt.start == Sqlx.NULL || start == dt.start) &&
                (dt.end == Sqlx.NULL || end == dt.end) && (dt.charSet == CharSet.UCS || charSet == dt.charSet) &&
                (dt.culture == CultureInfo.InvariantCulture || culture == dt.culture);
        }

        /// <summary>
        /// Output the actual obs type details if not the same as nominal obs type
        /// </summary>
        /// <param name="wr"></param>
        public virtual void PutDataType(Domain nt, Writer wr)
        {
            if (EqualOrStrongSubtypeOf(nt) && wr.cx.db.types.Contains(this) 
                && CompareTo(nt)!=0)
            {
                wr.WriteByte((byte)DataType.DomainRef);
                wr.PutLong(wr.cx.db.types[this]);
                return;
            }
            else 
                switch (Equivalent(kind))
                {
                    case Sqlx.Null:
                    case Sqlx.NULL: wr.WriteByte((byte)DataType.Null); break;
                    case Sqlx.ARRAY: wr.WriteByte((byte)DataType.Array); break;
                    case Sqlx.BLOB: wr.WriteByte((byte)DataType.Blob); break;
                    case Sqlx.BOOLEAN: wr.WriteByte((byte)DataType.Boolean); break;
                    case Sqlx.LEVEL:
                    case Sqlx.METADATA:
                    case Sqlx.CHECK:
                    case Sqlx.CHAR: wr.WriteByte((byte)DataType.String); break;
                    case Sqlx.DOCUMENT: goto case Sqlx.BLOB;
                    case Sqlx.DOCARRAY: goto case Sqlx.BLOB;
                    case Sqlx.OBJECT: goto case Sqlx.BLOB;
#if MONGO || SIMILAR
                case Sqlx.REGULAR_EXPRESSION: goto case Sqlx.CHAR;
#endif
                    case Sqlx.XML: goto case Sqlx.CHAR;
                    case Sqlx.INTEGER: wr.WriteByte((byte)DataType.Integer); break;
                    case Sqlx.MULTISET: wr.WriteByte((byte)DataType.Multiset); break;
                    case Sqlx.NUMERIC: wr.WriteByte((byte)DataType.Numeric); break;
                    case Sqlx.PASSWORD: wr.WriteByte((byte)DataType.Password); break;
                    case Sqlx.REAL: wr.WriteByte((byte)DataType.Numeric); break;
                    case Sqlx.DATE: wr.WriteByte((byte)DataType.Date); break;
                    case Sqlx.TIME: wr.WriteByte((byte)DataType.TimeSpan); break;
                    case Sqlx.TIMESTAMP: wr.WriteByte((byte)DataType.TimeStamp); break;
                    case Sqlx.INTERVAL: wr.WriteByte((byte)DataType.Interval); break;
                    case Sqlx.TYPE:
                        wr.WriteByte((byte)DataType.DomainRef);
                        var nd = (Domain)wr.cx.db.objects[defpos]; // without names
                        wr.PutLong(wr.cx.db.types[nd]); break;
                    case Sqlx.REF:
                    case Sqlx.ROW: wr.WriteByte((byte)DataType.Row); break;
                }
        }
        public virtual void Put(TypedValue tv, Writer wr)
        {
            switch (Equivalent(kind))
            {
                case Sqlx.SENSITIVE: elType.Put(tv, wr); break;
                case Sqlx.NULL: break;
                case Sqlx.BLOB: wr.PutBytes((byte[])tv.Val()); break;
                case Sqlx.BOOLEAN: wr.WriteByte((byte)(tv.ToBool().Value ? 1 : 0)); break;
                case Sqlx.CHAR: wr.PutString(tv?.ToString()); break;
                case Sqlx.DOCUMENT:
                    {
                        var d = tv as TDocument;
                        wr.PutBytes(d.ToBytes(null)); break;
                    }
                case Sqlx.INCREMENT:
                    {
                        var d = tv as Delta;
                        wr.PutInt((int)d.details.Count);
                        for (var b=d.details.First();b!=null;b=b.Next())
                        {
                            var de = b.value();
                            wr.PutInt(de.ix);
                            wr.WriteByte((byte)de.how);
                            wr.PutString(de.name.ToString());
                            var dt = de.what.dataType;
                            dt.Put(de.what, wr);
                        }
                        break;
                    }
#if MONGO
                case Sqlx.OBJECT: PutBytes(p, ((TObjectId)v).ToBytes()); break;
#endif
                case Sqlx.DOCARRAY:
                    {
                        var d = tv as TDocArray;
                        wr.PutBytes(d.ToBytes()); break;
                    }
                case Sqlx.PASSWORD: goto case Sqlx.CHAR;
#if SIMILAR
                case Sqlx.REGULAR_EXPRESSION: goto case Sqlx.CHAR;
#endif
                case Sqlx.XML: goto case Sqlx.CHAR;
                case Sqlx.INTEGER:
                    {
                        var n = tv as TInteger;
                        if (n == null)
                            wr.PutLong(tv.ToLong().Value);
                        else
                            wr.PutInteger(n.ivalue);
                        break;
                    }
                case Sqlx.NUMERIC:
                    {
                        var d = tv.Val() as Numeric;
                        if (tv is TInt)
                            d = new Numeric(tv.ToLong().Value);
                        if (tv is TInteger)
                            d = new Numeric((Integer)tv.Val(), 0);
                        wr.PutInteger(d.mantissa);
                        wr.PutInt(d.scale);
                        break;
                    }
                case Sqlx.REAL:
                    {
                        Numeric d;
                        if (tv == null)
                            break;
                        if (tv is TReal)
                            d = new Numeric(tv.ToDouble());
                        else
                            d = (Numeric)tv.Val();
                        wr.PutInteger(d.mantissa);
                        wr.PutInt(d.scale);
                        break;
                    }
                case Sqlx.DATE:
                    if (tv is TInt)
                    {
                        wr.PutLong(tv.ToLong().Value);
                        return;
                    }
                    wr.PutLong((((TDateTime)tv).value.Value).Ticks); break;
                case Sqlx.TIME:
                    if (tv is TInt)
                    {
                        wr.PutLong(tv.ToLong().Value);
                        return;
                    }
                    wr.PutLong((((TTimeSpan)tv).value.Value).Ticks); break;
                case Sqlx.TIMESTAMP:
                    if (tv is TInt)
                    {
                        wr.PutLong(tv.ToLong().Value);
                        return;
                    }
                    wr.PutLong((((TDateTime)tv).value.Value).Ticks); break;
                case Sqlx.INTERVAL:
                    {
                        Interval n = null;
                        if (tv is TInt) // shouldn't happen!
                            n = new Interval(tv.ToLong().Value);
                        else
                            n = (Interval)tv.Val();
                        wr.WriteByte(n.yearmonth ? (byte)1 : (byte)0);
                        if (n.yearmonth)
                        {
                            wr.PutInt(n.years);
                            wr.PutInt(n.months);
                        }
                        else
                            wr.PutLong(n.ticks);
                        break;
                    }
                case Sqlx.ROW:
                    {
                        if (tv is TArray ta)
                        {
                            if (ta.Length >= 1)
                                tv = ta[0];
                            else
                                break;
                        }
                        tv = Coerce(wr.cx, tv);
                        var rw = tv as TRow;
                        wr.PutLong(defpos);
                        var st = rw.dataType;
                        var cs = rw.columns;
                        wr.PutInt(cs.Length);
                        for (var b = cs.First(); b != null; b = b.Next())
                        {
                            var p = b.value();
                            var n = st.NameFor(wr.cx, p, b.key());
                            wr.PutString(n);
                            wr.cx._Dom(st.representation[p]).Put(rw[p], wr);
                        }
                        break;
                    }
                case Sqlx.TYPE:
                    {
                        tv = Coerce(wr.cx, tv);
                        var rw = tv as TRow;
                        wr.PutLong(defpos);
                        var st = rw.dataType;
                        var rs = st.representation;
                        wr.PutInt((int)rs.Count);
                        var j = 0;
                        for (var b = rs.First(); b != null; b = b.Next(),j++)
                        {
                            var p = b.key();
                            var n = st.NameFor(wr.cx, p, j);
                            wr.PutString(n);
                            b.value().Put(rw[p], wr);
                        }
                        break;
                    }
                case Sqlx.REF: goto case Sqlx.ROW;
                case Sqlx.ARRAY:
                    {
                        var a = (TArray)tv;
                        var et = a.dataType.elType;
                        wr.PutLong(wr.cx.db.types[et]);
                        wr.PutInt(a.Length);
                        for(var b=a.list.First();b!=null;b=b.Next())
                            et.Put(b.value(), wr);
                        break;
                    }
                case Sqlx.MULTISET:
                    {
                        TMultiset m = (TMultiset)tv;
                        var et = m.dataType.elType;
                        wr.PutLong(wr.cx.db.types[et]);
                        wr.PutInt((int)m.Count);
                        for (var a = m.tree.First(); a != null; a = a.Next())
                            for (int i = 0; i < a.value(); i++)
                                et.Put(a.key(), wr);
                        break;
                    }
                case Sqlx.METADATA:
                    {
                        var m = (TMetadata)tv;
                        wr.PutString(m.ToString());
                        break;
                    }
            }
        }
        protected static int Comp(IComparable a,IComparable b)
        {
            if (a == null && b == null)
                return 0;
            if (a == null)
                return -1;
            if (b == null)
                return 1;
            return a.CompareTo(b);
        }
        public virtual int CompareTo(object obj)
        {
            if (obj == null)
                return 1;
            var that = (Domain)obj;
            var c = kind.CompareTo(that.kind);
            if (c != 0)
                return c;
            c = Comp(iri, that.iri);
            if (c != 0)
                return c;
            c = Comp(abbrev, that.abbrev);
            if (c != 0)
                return c;
            c = Comp(constraints, that.constraints);
            if (c != 0)
                return c;
            c = Comp(defaultString, that.defaultString);
            if (c != 0)
                return c;
            c = Compare(defaultValue, that.defaultValue);
            if (c != 0)
                return c;
            switch (kind) {
                case Sqlx.Null:
                case Sqlx.VALUE:
                case Sqlx.CONTENT:
                case Sqlx.BLOB:
                case Sqlx.BOOLEAN:
                case Sqlx.XML: 
                    return 0;
                case Sqlx.UNION:
                case Sqlx.TABLE:
                case Sqlx.ROW:
                    return defpos.CompareTo(that.defpos);
                case Sqlx.TYPE:
                    return name.CompareTo(that.name);
                case Sqlx.CHAR:
                case Sqlx.NCHAR:
                case Sqlx.PASSWORD:
                case Sqlx.CLOB:
                case Sqlx.NCLOB:
                    c = Comp(culture.Name, that.culture.Name);
                    if (c != 0)
                        return c;
                    c = Comp(prec, that.prec);
                    return c;
                case Sqlx.NUMERIC:
                case Sqlx.DECIMAL:
                case Sqlx.DEC:
                    c = Comp(scale, that.scale);
                    if (c != 0)
                        return c;
                    goto case Sqlx.INT;
                case Sqlx.INT:
                case Sqlx.SMALLINT:
                case Sqlx.BIGINT:
                case Sqlx.INTEGER:
                case Sqlx.FLOAT:
                case Sqlx.DOUBLE:
                case Sqlx.REAL:
                    c = Comp(prec, that.prec);
                    return c;
                case Sqlx.DATE:
                case Sqlx.TIME:
                case Sqlx.TIMESTAMP:
                case Sqlx.INTERVAL:
                    c = Comp(start, that.start);
                    if (c != 0)
                        return c;
                    c = Comp(end, that.end);
                    return c;
                default:
                    return ToString().CompareTo(that.ToString());
            }
        } 
        /// <summary>
        /// Compare two values of this type.
        /// (v5.1 allow the second to have type Document in all cases)
        /// </summary>
        /// <param name="a">the first value</param>
        /// <param name="b">the second value</param>
        /// <returns>-1,0,1 according as a LT,EQ,GT b</returns>
        public virtual int Compare(TypedValue a, TypedValue b)
        {
            var an = a == null || a.IsNull;
            var bn = b == null || b.IsNull;
            if (an && bn)
                return 0;
            if (an)
                return (nulls == Sqlx.FIRST) ? 1 : -1;
            if (bn)
                return (nulls == Sqlx.FIRST) ? -1 : 1;
            int c;
            if (orderflags != Common.OrderCategory.None && orderflags != Common.OrderCategory.Primitive)
            {
                var cx = Context._system;
                var sa = new SqlLiteral(cx.GetUid(),cx,a);
                cx.Add(sa);
                var sb = new SqlLiteral(cx.GetUid(),cx,b);
                cx.Add(sb);
                if ((orderflags & Common.OrderCategory.Relative) == Common.OrderCategory.Relative)
                {
                    orderFunc.Exec(cx, new CList<long>(sa.defpos) + sb.defpos);
                    c = cx.val.ToInt().Value;
                    goto ret;
                }
                orderFunc.Exec(cx,new CList<long>(sa.defpos));
                a = cx.val;
                orderFunc.Exec(cx,new CList<long>(sb.defpos));
                b = cx.val;
                c = a.dataType.Compare(a, b);
                goto ret;
            }
            switch (Equivalent(kind))
            {
                case Sqlx.BOOLEAN: return (a.ToBool().Value).CompareTo(b.ToBool().Value);
                case Sqlx.CHAR:
                    {
                        if (a is TInt ai)
                        {
                            if (b is TInt bi)
                                c = ai.ToInteger().CompareTo(bi.ToInteger());
                            else
                                c = ai.ToInteger().CompareTo(Integer.Parse(b.ToString()));
                        }
                        else if (b is TInt bj)
                            c = Integer.Parse(a.ToString()).CompareTo(bj.ToInteger());
                        else
                            c = string.Compare(a.ToString(), b.ToString(), false, culture);
                        break;
                    }
                case Sqlx.XML: goto case Sqlx.CHAR;
                case Sqlx.INTEGER:
                    if (a.Val() is long)
                    {
                        if (b.Val() is long)
                            c = a.ToLong().Value.CompareTo(b.ToLong().Value);
                        else
                            c = new Integer(a.ToLong().Value).CompareTo(b.Val());
                    }
                    else if (b.Val() is long)
                        c = ((Integer)a.Val()).CompareTo(new Integer(b.ToLong().Value));
                    else
                        c = ((Integer)a.Val()).CompareTo((Integer)b.Val());
                    break;
                case Sqlx.NUMERIC:
                    if (a.Val() is long)
                    {
                        if (b.Val() is long)
                            c = a.ToLong().Value.CompareTo(b.ToLong().Value);
                        else
                            c = new Numeric(a.ToLong().Value).CompareTo(b.Val());
                    }
                    else if (b.Val() is long)
                        c = ((Numeric)a.Val()).CompareTo(new Numeric(b.ToLong().Value));
                    else
                        c = ((Numeric)a.Val()).CompareTo(b.Val()); break;
                case Sqlx.REAL:
                    var da = a.ToDouble();
                    var db = b.ToDouble();
                    c = da.CompareTo(db); break;
                case Sqlx.DATE:
                    {
                        var oa = a.Val();
                        if (oa is long)
                            oa = new DateTime((long)oa);
                        if (oa is DateTime)
                            oa = new Date((DateTime)oa);
                        var ob = b.Val();
                        if (ob is long)
                            ob = new DateTime((long)ob);
                        if (ob is DateTime)
                            ob = new Date((DateTime)ob);
                        c = (((Date)oa).date).CompareTo(((Date)ob).date);
                        break;
                    }
                case Sqlx.DOCUMENT:
                    {
                        var dcb = a as TDocument;
                        c = dcb.RowSet(b);
                        break;
                    }
                case Sqlx.CONTENT: c = a.ToString().CompareTo(b.ToString()); break;
                case Sqlx.TIME: c = ((TimeSpan)a.Val()).CompareTo(b.Val()); break;
                case Sqlx.TIMESTAMP:
                    c = ((DateTime)a.Val()).CompareTo((DateTime)b.Val());
                    break;
                case Sqlx.INTERVAL:
                    {
                        var ai = (Interval)a.Val();
                        var bi = (Interval)b.Val();
                        if (ai.yearmonth != bi.yearmonth)
                            throw new DBException("22202");
                        if (ai.yearmonth)
                        {
                            c = ai.years.CompareTo(bi.years);
                            if (c != 0)
                                break;
                            c = ai.months.CompareTo(bi.months);
                        }
                        else
                            c = ai.ticks.CompareTo(bi.ticks);
                        break;
                    }
                case Sqlx.ARRAY:
                    {
                        var x = a as TArray;
                        var y = b as TArray;
                        var xe = Context._system._Dom(x.dataType.elType); 
                        if (x == null || y == null)
                            throw new DBException("22004").ISO();
                        if (x.dataType.elType != y.dataType.elType)
                            throw new DBException("22202").Mix()
                                .AddType(xe).AddValue(y.dataType);
                        int n = x.Length;
                        int m = y.Length;
                        if (n != m)
                        {
                            c = (n < m) ? -1 : 1;
                            break;
                        }
                        c = 0;
                        for (int j = 0; j < n; j++)
                        {
                            c = xe.Compare(x[j], y[j]);
                            if (c != 0)
                                break;
                        }
                        break;
                    }
                case Sqlx.MULTISET:
                    {
                        var x = a as TMultiset;
                        var y = b as TMultiset;
                        var xe = Context._system._Dom(x.dataType.elType);
                        var ye = Context._system._Dom(y.dataType.elType);
                        if (x == null || y == null)
                            throw new DBException("22004").ISO();
                        if (x.dataType.elType != y.dataType.elType)
                            throw new DBException("22202").Mix()
                                .AddType(xe)
                                .AddValue(ye);
                        var e = x.tree.First();
                        var f = y.tree.First();
                        for (; ; )
                        {
                            if (e == null || f == null)
                                break;
                            c = xe.Compare(e.key(), f.key());
                            if (c != 0)
                                goto ret;
                            c = e.value().CompareTo(f.value());
                            if (c != 0)
                                goto ret;
                            break;
                        }
                        c = (e == null) ? ((f == null) ? 0 : -1) : 1;
                        break;
                    }
#if SIMILAR
                case Sqlx.REGULAR_EXPRESSION:
                    {
                        c = RegEx.PCREParse(a.ToString()).Like(b.ToString(), null) ? 0 : 1;
                        break;
                    }
#endif
                case Sqlx.ROW:
                    {
                        TRow ra = a as TRow;
                        TRow rb = b as TRow;
                        if (ra == null || rb == null)
                            throw new DBException("22004").ISO();
                        c = 0;
                        var bb = rb.dataType.rowType.First();
                        var cb = ra.dataType.rowType.First();
                        for (;c == 0 && cb != null && bb!=null;cb = cb.Next(),bb=bb.Next())
                        {
                            var k = cb.key();
                            var dm = ra.dataType.representation[cb.value()];
                            c = dm.Compare(ra[k], rb[k]);
                        }
                        if (c != 0)
                            return c;
                        if (cb != null)
                            return 1;
                        if (bb != null)
                            return -1;
                        return 0;
                    }
                case Sqlx.PERIOD:
                    {
                        var pa = a.Val() as Period;
                        var pb = b.Val() as Period;
                        c = elType.Compare(pa.start, pb.start);
                        if (c == 0)
                            c = elType.Compare(pa.end, pb.end);
                        break;
                    }
                case Sqlx.UNION:
                    {
                        for (var bb = unionOf.First(); bb != null; bb = bb.Next())
                            if (bb.key() is Domain dt)
                                if (dt.CanBeAssigned(a) && dt.CanBeAssigned(b))
                                    return dt.Compare(a, b);
                        throw new DBException("22202", a.dataType.ToString(), b.dataType.ToString());
                    }
                case Sqlx.PASSWORD:
                    throw new DBException("22202").ISO();
                default:
                    if (orderflags == Common.OrderCategory.Primitive)
                        c = ((IComparable)a.Val()).CompareTo(b.Val());
                    else
                        c = a.ToString().CompareTo(b.ToString()); break;
            }
            ret:
            return (AscDesc==Sqlx.DESC)?-c:c;
        }
        internal int Compare(CTree<long,TypedValue> a,CTree<long,TypedValue>b)
        {
            int c = 0;
            for (var bm=rowType.First();c==0 && bm!=null;bm=bm.Next())
            {
                var p = bm.value();
                c = representation[p].Compare(a[p], b[p]);
            }
            return c;
        }
        /// <summary>
        /// Creator: Add the given array at the end of this
        /// </summary>
        /// <param name="a"></param>
        /// <returns></returns>
        public TArray Concatenate(TArray a, TArray b)
        {
            var r = new TArray(this);
            var et = elType;
            var ae = a.dataType.elType;
            var be = b.dataType.elType;
            if (ae != et || ae != be)
                throw new DBException("22102").Mix();
            for(var ab=a.list.First();ab!=null;ab=ab.Next())
                r += ab.value();
            for(var bb = b.list.First();bb!=null;bb=bb.Next())
                r += bb.value();
            return r;
        }
        /// <summary>
        /// Test a given type to see if its values can be assigned to this type
        /// </summary>
        /// <param name="dt">The other obs type</param>
        /// <returns>whether values of the given type can be assigned to a variable of this type</returns>
        public virtual bool CanTakeValueOf(Domain dt)
        {
            if (sensitive != dt.sensitive)
                 return false;
            if (kind == Sqlx.VALUE || kind == Sqlx.CONTENT)
                return true;
            if (dt.kind == Sqlx.CONTENT || dt.kind == Sqlx.VALUE)
                return kind != Sqlx.REAL && kind != Sqlx.INTEGER && kind != Sqlx.NUMERIC;
            if (kind == Sqlx.ANY)
                return true;
            if ((dt.kind == Sqlx.TABLE || dt.kind == Sqlx.ROW) && dt.rowType.Length == 1
                && CanTakeValueOf(dt.representation[dt.rowType[0]]))
                return true;
            if (kind == Sqlx.UNION && dt.kind==Sqlx.UNION)
            {
                for (var b = unionOf.First(); b != null; b = b.Next())
                    if (dt.unionOf.Contains(b.key()))
                        return true;
                return false;
            }
            if (kind == Sqlx.UNION && dt!=Content)
            {
                for (var b = unionOf.First(); b != null; b = b.Next())
                    if (b.key().CanTakeValueOf(dt))
                        return true;
                return false;
            }
            if (display>0 && display==dt.display)
            {
                var e = rowType.First();
                var c = true;
                for (var te = dt.rowType.First(); c && e != null && te != null;
                    e = e.Next(), te = te.Next())
                {
                    if (e.key() >= display)
                        break;
                    var d = representation[e.value()];
                    var td = dt.representation[te.value()];
                    c = d.CanTakeValueOf(td);
                }
                if (c)
                    return true;
            }
            var ki = Equivalent(kind);
            var dk = Equivalent(dt.kind);
            switch (ki)
            {
                default: return ki == dk;
                case Sqlx.CONTENT: return true;
                case Sqlx.NCHAR: return dk == Sqlx.CHAR || dk == ki;
                case Sqlx.NUMERIC: return dk == Sqlx.INTEGER || dk == ki;
                case Sqlx.PASSWORD: return dk == Sqlx.CHAR || dk == ki;
                case Sqlx.REAL: return dk == Sqlx.INTEGER || dk == Sqlx.NUMERIC || dk == ki;
                case Sqlx.TABLE:
                case Sqlx.TYPE:
                case Sqlx.ROW:
                    return rowType.Length==0 ||CompareTo(dt)==0; // generic TABLE etc is ok
                case Sqlx.ARRAY:
                case Sqlx.MULTISET:
                    return CompareTo(dt)==0;

#if SIMILAR
                case Sqlx.CHAR: return dk == Sqlx.REGULAR_EXPRESSION || dk == ki;
#endif
            }
        }
        public virtual bool HasValue(Context cx,TypedValue v)
        {
            if (v is TNull)
                return true;
            if (v is TSensitive st)
            {
                if (sensitive)
                    return HasValue(cx,st.value);
                return false;
            }
            var ki = Equivalent(kind);
            if (ki == Sqlx.ONLY || iri != null)
                return Equals(v.dataType); // must match exactly
            if (ki == Sqlx.NULL || kind == Sqlx.ANY)
                return true;
            if (ki == Sqlx.UNION)
                return (mem.Contains(cx.db.types[v.dataType]));
            if (ki != v.dataType.kind)
                return false;
            switch (ki)
            {
                case Sqlx.MULTISET:
                case Sqlx.ARRAY:
                    return elType == v.dataType.elType;
                case Sqlx.TABLE:
                case Sqlx.TYPE:
                case Sqlx.ROW:
                    {
                        var vr = v as TRow;
                        var dt = v.dataType;
                        if (vr == null)
                            return false;
                        if (dt.Length != Length)
                            return false;
                        var b = rowType.First();
                        for (var vb = dt.rowType.First(); b!=null && vb!=null; b=b.Next(),vb.Next())
                        {
                            var vc = vb.value();
                            var vd = cx._Dom(dt.representation[vc]);
                            var c = b.value();
                            var cd = cx._Dom(representation[c]);
                            if (!vd.EqualOrStrongSubtypeOf(cd))
                                return false;
                        }
                        break;
                    }
            }
            return true;
        }
        public virtual TypedValue Parse(long off,string s)
        {
            if (s == null)
                return TNull.Value;
            if (sensitive)
                return new TSensitive(this, Parse(new Scanner(off,s.ToCharArray(),0)));
            if (kind == Sqlx.DOCUMENT)
                return new TDocument(s);
            if (kind== Sqlx.METADATA)
                return new TMetadata(new Parser(Database._system,new Connection())
                    .ParseMetadata(s,(int)off,Sqlx.VIEW));
            return Parse(new Scanner(off,s.ToCharArray(), 0));
        }
        public CTree<long, TypedValue> Parse(Context cx, string s)
        {
            var psr = new Parser(cx, s);
            var vs = CTree<long, TypedValue>.Empty;
            if (psr.tok == Sqlx.LPAREN)
                psr.Next();
            // might be named or positional
            var ns = CTree<string, long>.Empty;
            for (var b = rowType.First(); b != null; b = b.Next())
            {
                var rp = b.value();
                var ci = (ObInfo)cx.db.role.infos[rp];
                ns += (ci.name, rp);
            }
            for (var j = 0; j < rowType.Length;)
            {
                var a = psr.lxr.val.ToString();
                if (psr.lxr.val is TChar tx && ns.Contains(tx.value))
                {
                    psr.Next();
                    psr.Mustbe(Sqlx.EQL);
                        if (!ns.Contains(a))
                            throw new DBException("42000", a);
                        var p = ns[a];
                        if (psr.ParseSqlValueItem(representation[p], false) is SqlLiteral sl)
                            vs += (p, sl.val);
                        else throw new DBException("42000", a);
                }
                else
                {
                    var rj = rowType[j++];
                    var sv = psr.ParseSqlValueItem(representation[rj], false);
                    if (sv is SqlLiteral v)
                        vs += (rj, v.val);
                }
                if (psr.tok != Sqlx.COMMA)
                    break;
                psr.Next();
            }
            return vs;
        }
        public virtual TypedValue Parse(long off,string s, string m, Context cx)
        {
            if (kind == Sqlx.TABLE && m == "application/json")
            {
                if (s == "") 
                    s = "[]";
                return Coerce(cx, new DocArray(s));
            }
            if (sensitive)
                return new TSensitive(this, Parse(new Scanner(off, s.ToCharArray(), 0, m,cx)));
            if (kind == Sqlx.DOCUMENT)
                return new TDocument(s);
            return Parse(new Scanner(off,s.ToCharArray(), 0, m,cx));
        }
        /// <summary>
        /// Parse a string value for this type. 
        /// </summary>
        /// <param name="lx">The scanner</param>
        /// <returns>a typedvalue</returns>
        public TypedValue Parse(Scanner lx, bool union = false)
        {
            TypedValue v;
            int start = lx.pos;
            if (TryParse(lx, out v, union) is DBException ex)
                throw ex;
            return v;
        }
        public DBException TryParse(Scanner lx,out TypedValue v,bool union=false)
        {
            v = TNull.Value;
            if (lx.len == 0)
            { v = new TRow(lx._cx, this); return null; }
            if (sensitive)
            { v = new TSensitive(this, Parse(lx, union)); return null; }
            int start = lx.pos;
            if (lx.Match("null"))
            { v = TNull.Value; return null; }
            switch (Equivalent(kind))
            {
                case Sqlx.Null:
                    {
                        int st = lx.pos;
                        int ln = lx.len - lx.pos;
                        var str = new string(lx.input, st, ln);
                        var lxr = new Lexer(str);
                        lx.pos += lxr.pos;
                        lx.ch = (lxr.pos>=lxr.input.Length)?(char)0:lxr.input[lxr.pos];
                        { v = lxr.val; return null; }
                    }
                case Sqlx.BOOLEAN:
                    if (lx.MatchNC("TRUE"))
                    { v = TBool.True; return null; }
                    if (lx.MatchNC("FALSE"))
                    { v = TBool.False; return null; }
                    break;
                case Sqlx.CHAR:
                    {
                        int st = lx.pos;
                        int ln = lx.len - lx.pos;
                        var str = new string(lx.input, st, ln);
                        var qu = lx.ch;
                        if (qu == '\'' || qu == '"' || qu == (char)8217)
                        {
                            var sb = new StringBuilder();
                            while (lx.pos < lx.len && lx.ch == qu)
                            {
                                lx.Advance();
                                while (lx.pos < lx.len && lx.ch != qu)
                                {
                                    sb.Append(lx.ch);
                                    lx.Advance();
                                }
                                lx.Advance();
                                if (lx.pos < lx.len && lx.ch == qu)
                                    sb.Append(lx.ch);
                            }
                            str = sb.ToString();
                        }
                        else if (str.StartsWith("null"))
                        {
                            for (var i = 0; i < 4; i++)
                                lx.Advance();
                            v =TNull.Value; return null;
                        }
                        else
                        {
                            lx.pos = lx.len;
                            lx.ch = '\0';
                        }
                        if (prec != 0 && prec < str.Length)
                            str = str.Substring(0, prec);
                        if (charSet == CharSet.UCS || Check(str))
                        { v = new TChar(str); return null; }
                        break;
                    }
                case Sqlx.CONTENT:
                    {
                        var st = lx.pos;
                        var s = new string(lx.input, lx.pos, lx.input.Length - lx.pos);
                        var i = 1;
                        var c = TDocument.GetValue(null, s, s.Length, ref i);
                        lx.pos = lx.pos + i;
                        lx.ch = (lx.pos < lx.input.Length) ? lx.input[lx.pos] : '\0';
                        { v = c.Item2; return null; }
                    }
                case Sqlx.PASSWORD: goto case Sqlx.CHAR;
                /*                case Sqlx.XML:
                                    {
                                        TXml rx = null;
                                        var xr = XmlReader.Create(new StringReader(new string(lx.input, start, lx.input.Length - start)));
                                        while (xr.Read())
                                            switch (xr.NodeType)
                                            {
                                                case XmlNodeType.Element:
                                                    if (rx == null)
                                                    {
                                                        rx = new TXml(xr.Value);
                                                        if (xr.HasAttributes)
                                                        {
                                                            var an = xr.AttributeCount;
                                                            for (int i = 0; i < an; i++)
                                                            {
                                                                xr.MoveToAttribute(i);
                                                                rx = new TXml(rx, xr.Name, new TChar(xr.Value));
                                                            }
                                                        }
                                                        xr.MoveToElement();
                                                    }
                                                    rx.children.Add(Parse(new Scanner(lx.tr, xr.ReadInnerXml().ToCharArray(), 0)) as TXml);
                                                    break;
                                                case XmlNodeType.Text:
                                                    rx = new TXml(rx, xr.Value);
                                                    break;
                                                case XmlNodeType.EndElement:
                                                    return rx;
                                            }
                                        break;
                                    }*/
                case Sqlx.NUMERIC:
                    {
                        string str;
                        if (char.IsDigit(lx.ch) || lx.ch == '-' || lx.ch == '+')
                        {
                            start = lx.pos;
                            lx.Advance();
                            while (char.IsDigit(lx.ch))
                                lx.Advance();
                            if (lx.ch == '.' && kind != Sqlx.INTEGER)
                            {
                                lx.Advance();
                                while (char.IsDigit(lx.ch))
                                    lx.Advance();
                            }
                            else
                            {
                                str = lx.String(start, lx.pos - start);
                                if (lx.pos - start > 18)
                                {
                                    Integer x = Integer.Parse(str);
                                    if (kind == Sqlx.NUMERIC)
                                    { v = new TNumeric(this, new Common.Numeric(x, 0)); return null; }
                                    if (kind == Sqlx.REAL)
                                    { v = new TReal(this, (double)x); return null; }
                                    if (lx.ch == '.') // tolerate .00000
                                    {
                                        if (union)
                                            throw new InvalidCastException();
                                        var first = true;
                                        lx.Advance();
                                        if (first && lx.ch > '5')  // >= isn't entirely satisfactory either
                                        {
                                            if (x >= 0)
                                                x = x + Integer.One;
                                            else
                                                x = x - Integer.One;
                                        }
                                        else
                                            first = false;
                                        while (char.IsDigit(lx.ch))
                                            lx.Advance();
                                    }
                                    v = new TInt(this, x); return null;
                                }
                                else
                                {
                                    long x = long.Parse(str);
                                    if (kind == Sqlx.NUMERIC)
                                    { v = new TNumeric(this, new Common.Numeric(x)); return null; }
                                    if (kind == Sqlx.REAL)
                                    { v = new TReal(this, (double)x); return null; }
                                    if (lx.ch == '.') // tolerate .00000
                                    {
                                        //            if (union)
                                        //                throw new InvalidCastException();
                                        var first = true;
                                        lx.Advance();
                                        if (first && lx.ch > '5') // >= isn't entirely satisfactory either
                                        {
                                            if (x >= 0)
                                                x++;
                                            else
                                                x--;
                                        }
                                        else
                                            first = false;
                                        while (char.IsDigit(lx.ch))
                                            lx.Advance();
                                    }
                                    v = new TInt(this, x); return null;
                                }
                            }
                            if ((lx.ch != 'e' && lx.ch != 'E') || kind == Sqlx.NUMERIC)
                            {
                                str = lx.String(start, lx.pos - start);
                                Common.Numeric x = Common.Numeric.Parse(str);
                                if (kind == Sqlx.REAL)
                                    v = new TReal(this, (double)x);
                                else
                                    v = new TNumeric(this, x);
                                return null;
                            }
                            lx.Advance();
                            if (lx.ch == '-' || lx.ch == '+')
                                lx.Advance();
                            if (!char.IsDigit(lx.ch))
                                return new DBException("22107").Mix();
                            lx.Advance();
                            while (char.IsDigit(lx.ch))
                                lx.Advance();
                            str = lx.String(start, lx.pos - start);
                            v = new TReal(this, (double)Common.Numeric.Parse(str));
                            return null;
                        }
                    }
                    break;
                case Sqlx.INTEGER: goto case Sqlx.NUMERIC;
                case Sqlx.REAL: goto case Sqlx.NUMERIC;
                case Sqlx.DATE:
                    {
                        var st = lx.pos;
                        var da = GetDate(lx, st);
                        if (lx.ch == 'T' || lx.ch == ' ') // tolerate unnecessary time information
                        {
                            lx.Advance();
                            GetTime(lx, st);
                        }
                        v = new TDateTime(this, da);
                        return null;
                    }
                case Sqlx.TIME: v = new TTimeSpan(this, GetTime(lx, lx.pos)); return null;
                case Sqlx.TIMESTAMP: v = new TDateTime(this, GetTimestamp(lx, lx.pos)); return null;
                case Sqlx.INTERVAL: v = new TInterval(this, GetInterval(lx)); return null;
                case Sqlx.TYPE:
                case Sqlx.TABLE:
                case Sqlx.VIEW:
                    v = (this+(Kind,Sqlx.ROW)).ParseList(lx); return null;
                case Sqlx.ROW:
                    {
                        if (lx.mime == "text/xml")
                        {
                            // tolerate missing values and use of attributes
                            var cols = CTree<long, TypedValue>.Empty;
                            var xd = new XmlDocument();
                            xd.LoadXml(new string(lx.input));
                            var xc = xd.FirstChild;
                            if (xc != null && xc is XmlDeclaration)
                                xc = xc.NextSibling;
                            if (xc == null)
                                goto bad;
                            bool blank = true;
                            for (var b = rowType.First(); b != null; b = b.Next())
                            {
                                var p = b.value();
                                var cn = (lx._cx.db.role.infos[p] is ObInfo co) ? co.name
                                    : (lx._cx.obs[p] is SqlValue sv) ? sv.name
                                    : b.key().ToString();
                                var cd = lx._cx._Dom(representation[p]);
                                TypedValue item = null;
                                if (xc.Attributes != null)
                                {
                                    var att = xc.Attributes[cn];
                                    if (att != null)
                                        item = cd.Parse(0, att.InnerXml, lx.mime, lx._cx);
                                }
                                if (item == null)
                                    for (int j = 0; j < xc.ChildNodes.Count; j++)
                                    {
                                        var xn = xc.ChildNodes[j];
                                        if (xn.Name == (cn))
                                        {
                                            item = cd.Parse(0, xn.InnerXml, lx.mime, lx._cx);
                                            break;
                                        }
                                    }
                                blank = blank && (item == null);
                                cols += (b.value(), item);
                            }
                            if (blank)
                                v = TXml.Null;
                            else
                                v = new TRow(this, cols);
                            return null;
                        }
                        else
                            if (lx.mime == "text/csv")
                        {
                            // we expect all columns, separated by commas, without string quotes
                            var vs = CTree<long, TypedValue>.Empty;
                            for (var b = rowType.First(); b != null; b = b.Next())
                            {
                                // for this mime type we only understand primitive types
                                var cd = representation[b.value()] ?? Content;
                                TypedValue vl = null;
                                try
                                {
                                    switch (cd.kind)
                                    {
                                        case Sqlx.CHAR:
                                            {
                                                int st = lx.pos;
                                                string s = "";
                                                if (lx.ch == '"')
                                                {
                                                    lx.Advance();
                                                    st = lx.pos;
                                                    while (lx.ch != '"')
                                                        lx.Advance();
                                                    s = new string(lx.input, st, lx.pos - st);
                                                    lx.Advance();
                                                }
                                                else
                                                {
                                                    while (lx.ch != ',' && lx.ch != '\n' && lx.ch != '\r')
                                                        lx.Advance();
                                                    s = new string(lx.input, st, lx.pos - st);
                                                }
                                                vl = new TChar(s);
                                                break;
                                            }
                                        case Sqlx.DATE:
                                            {
                                                int st = lx.pos;
                                                char oc = lx.ch;
                                                string s = "";
                                                while (lx.ch != ',' && lx.ch != '\n' && lx.ch != '\r')
                                                    lx.Advance();
                                                s = new string(lx.input, st, lx.pos - st);
                                                if (s.IndexOf("/") >= 0)
                                                {
                                                    var sa = s.Split('/');
                                                    vl = new TDateTime(Domain.Date, new DateTime(int.Parse(sa[2]), int.Parse(sa[0]), int.Parse(sa[1])));
                                                    break;
                                                }
                                                lx.pos = st;
                                                lx.ch = oc;
                                                vl = cd.Parse(lx);
                                                break;
                                            }
                                        default: vl = cd.Parse(lx); break;
                                    }
                                }
                                catch (Exception)
                                {
                                    while (lx.ch != '\0' && lx.ch != ',' && lx.ch != '\r' && lx.ch != '\n')
                                        lx.Advance();
                                }
                                if (b.Next() != null)
                                {
                                    if (lx.ch != ',')
                                        return new DBException("42101", lx.ch).Mix();
                                    lx.Advance();
                                }
                                else
                                {
                                    if (lx.ch == ',')
                                        lx.Advance();
                                    if (lx.ch != '\0' && lx.ch != '\r' && lx.ch != '\n')
                                        return new DBException("42101", lx.ch).Mix();
                                    while (lx.ch == '\r' || lx.ch == '\n')
                                        lx.Advance();
                                }
                                vs += (b.value(), vl);
                            }
                            v = new TRow(this, vs);
                            return null;
                        }
                        else
                        {
                            //if (names.Length > 0)
                            //    throw new DBException("2200N");
                            //tolerate named columns in SQL version
                            //mixture of named and unnamed columns is not supported
                            var comma = '(';
                            var end = ')';
                            if (lx.ch == '{')
                            {
                                comma = '{'; end = '}';
                            }
                            if (lx.ch == '[')
                                goto case Sqlx.TABLE;
                            var cols = CTree<long, TypedValue>.Empty;
                            var names = CTree<string, long>.Empty;
                            var b = rowType.First();
                            for (; b != null; b = b.Next())
                            {
                                var p = b.value();
                                var dm = representation[p];
                                if (lx._cx.obs[b.value()] is SqlValue sv)
                                {
                                    names += (sv.name, p);
                                    if (sv.alias != "")
                                        names += (sv.alias, p);
                                }
                                else
                                {
                                    var co = (ObInfo)lx._cx.db.role?.infos[p];
                                    cols += (b.value(), dm.defaultValue);
                                    if (co != null)
                                        names += (co.name, p);
                                }
                            }
                            b = rowType.First();
                            bool namedOk = true;
                            bool unnamedOk = true;
                            lx.White();
                            while (lx.ch == comma)
                            {
                                if (b == null)
                                    return new DBException("22207");
                                lx.Advance();
                                lx.White();
                                var n = lx.GetName();
                                long p = b.value();
                                if (n == null) // no name supplied
                                {
                                    if (b == null || !unnamedOk)
                                        return new DBException("22208").Mix();
                                    namedOk = false;
                                    b = b.Next();
                                }
                                else // column name supplied
                                {
                                    if (lx.ch != ':')
                                        return new DBException("42124").Mix();
                                    else
                                        lx.Advance();
                                    if (!namedOk)
                                        return new DBException("22208").Mix()
                                            .Add(Sqlx.COLUMN_NAME, new TChar(n));
                                    unnamedOk = false;
                                    if (names[n] != p)
                                        p = names[n];
                                }
                                lx.White();
                                cols += (p, (representation[p] ?? Content).Parse(lx));
                                comma = ',';
                                lx.White();
                            }
                            if (lx.ch != end)
                                break;
                            lx.Advance();
                            v = new TRow(this, cols);
                            return null;
                        }
                    }
                case Sqlx.ARRAY:
                    { v = lx._cx._Dom(elType).ParseList(lx); return null; }
                case Sqlx.UNION:
                    {
                        int st = lx.pos;
                        char ch = lx.ch;
                        for (var b = mem.First(); b != null; b = b.Next())
                            if (b.value() is Domain dt)
                            {
                                if (dt.TryParse(lx, out v, true)==null)
                                { 
                                    lx.White();
                                    if (lx.ch == ']' || lx.ch == ',' || lx.ch == '}')
                                        return null;
                                }
                                lx.pos = st;
                                lx.ch = ch;
                            }
                        break;
                    }
                case Sqlx.LEVEL:
                    {
                        lx.MatchNC("LEVEL");
                        lx.White();
                        var min = 'D' - lx.ch;
                        lx.Advance();
                        lx.White();
                        var max = min;
                        if (lx.ch == '-')
                        {
                            lx.Advance();
                            lx.White();
                            max = 'D' - lx.ch;
                        }
                        lx.White();
                        var gps = CTree<string, bool>.Empty;
                        var rfs = CTree<string, bool>.Empty;
                        var rfseen = false;
                        if (lx.MatchNC("groups"))
                        {
                            lx.White();
                            while (lx.pos < lx.len)
                            {
                                var s = lx.NonWhite();
                                if (s.ToUpper().CompareTo("REFERENCES") == 0)
                                    rfseen = true;
                                else if (rfseen)
                                    rfs +=(s, true);
                                else
                                    gps+=(s, true);
                                lx.White();
                            }
                        }
                        v = TLevel.New(new Level((byte)min, (byte)max, gps, rfs));
                        return null;
                    }
            }
            if (lx.pos + 4 < lx.len && new string(lx.input, start, 4).ToLower() == "null")
            {
                for (int i = 0; i < 4; i++)
                    lx.Advance();
                v = TNull.Value;
                return null;
            }
        bad:
            var xs = new string(lx.input, start, lx.pos - start);
            v = TNull.Value;
            return new DBException("2E303", ToString(), xs).Pyrrho()
                .AddType(this).AddValue(new TChar(xs));
        }
        TypedValue ParseList(Scanner lx)
        {
            if (kind == Sqlx.SENSITIVE)
                return new TSensitive(this, lx._cx._Dom(elType).ParseList(lx));
            var vs = BList<TypedValue>.Empty;
            var end = ')';
            switch(lx.ch)
            {
                case '[': end = ']'; lx.Advance(); break;
                case '{': end = ')'; lx.Advance(); break;
                case '(': end = ')'; lx.Advance(); break;
            }
            for (;lx.ch!=end; )
            {
                lx.White();
                vs += Parse(lx);
                lx.White();
                if (lx.ch == ',')
                    lx.Advance();
                else
                    break;
                lx.White();
            }
            if (lx.ch == end)
                lx.Advance();
            lx.Advance();
            return new TArray(this, vs);
        }
        /// <summary>
        /// Helper for parsing Interval values
        /// </summary>
        /// <param name="lx">the scanner</param>
        /// <returns>an Interval</returns>
        Interval GetInterval(Scanner lx)
        {
            int y = 0, M = 0, d = 0, h = 0, m = 0;
            long s = 0;
            bool sign = false;
            if (lx.ch == '-')
                sign = true;
            if (lx.ch == '+' || lx.ch == '-')
                lx.Advance();
            int ks = IntervalPart(start);
            int ke = IntervalPart(end);
            if (ke < 0)
                ke = ks + 1;
            var st = lx.pos;
            string[] parts = GetParts(lx, ke - ks+1, st);
            if (ks <= 1)
            {
                if (ks == 0)
                    y = int.Parse(parts[0]);
                if (ks <= 1 && ke == 1)
                    M = int.Parse(parts[1 - ks]);
                if (sign)
                { y = -y; M = -M; }
                return new Interval(y, M);
            }
            if (ks <= 2 && ke > 2)
                d = int.Parse(parts[2 - ks]);
            if (ks <= 3 && ke > 3)
                h = int.Parse(parts[3 - ks]);
            if (ks <= 4 && ke > 4)
                m = int.Parse(parts[4 - ks]);
            if (ke > 5)
                s = (long)(double.Parse(parts[5 - ks]) * TimeSpan.TicksPerSecond);
            s = d * TimeSpan.TicksPerDay + h * TimeSpan.TicksPerHour +
                m * TimeSpan.TicksPerMinute + s;
            if (sign)
                s = -s;
            return new Interval(s);
        }
        /// <summary>
        /// Facilitate quick decoding of the interval fields
        /// </summary>
        internal static Sqlx[] intervalParts = new Sqlx[] { Sqlx.YEAR, Sqlx.MONTH, Sqlx.DAY, Sqlx.HOUR, Sqlx.MINUTE, Sqlx.SECOND };
        /// <summary>
        /// helper for encoding interval fields
        /// </summary>
        /// <param name="e">YEAR, MONTH, DAY, HOUR, MINUTE, SECOND</param>
        /// <returns>corresponding integer 0,1,2,3,4,5</returns>
        internal static int IntervalPart(Sqlx e)
        {
            switch (e)
            {
                case Sqlx.YEAR: return 0;
                case Sqlx.MONTH: return 1;
                case Sqlx.DAY: return 2;
                case Sqlx.HOUR: return 3;
                case Sqlx.MINUTE: return 4;
                case Sqlx.SECOND: return 5;
            }
            return -1;
        }
        /// <summary>
        /// Helper for parts of a date value
        /// </summary>
        /// <param name="lx">the scanner</param>
        /// <param name="n">the number of parts</param>
        /// <returns>n strings</returns>
        string[] GetParts(Scanner lx, int n, int st)
        {
            string[] r = new string[n];
            for (int j = 0; j < n; j++)
            {
                if (lx.pos > lx.len)
                    throw new DBException("22007", Diag(lx, st)).Mix();
                r[j] = GetPart(lx, st);
                if (j < n - 1)
                    lx.Advance();
            }
            return r;
        }
        /// <summary>
        /// Helper for extracting parts of a date value
        /// </summary>
        /// <param name="lx">the scanner</param>
        /// <returns>a group of digits as a string</returns>
        string GetPart(Scanner lx, int st)
        {
            st = lx.pos;
            lx.Advance();
            while (char.IsDigit(lx.ch))
                lx.Advance();
            return new string(lx.input, st, lx.pos - st);
        }
        /// <summary>
        /// Get the date part from the string
        /// </summary>
        /// <returns>the DateTime so far</returns>
        DateTime GetDate(Scanner lx, int st)
        {
            try
            {
                int y, m, d;
                int pos = lx.pos;
                // first look for SQL standard date format
                if (lx.pos + 10 <= lx.input.Length && lx.input[lx.pos + 4] == '-' && lx.input[lx.pos + 7] == '-')
                {
                    y = GetNDigits(lx, '-', 0, 4, pos);
                    m = GetNDigits(lx, '-', 1, 2, pos);
                    d = GetNDigits(lx, '-', 1, 2, pos);
                }
                else // try to use regional settings
                {
                    y = GetShortDateField(lx, 'y', ref pos, st);
                    m = GetShortDateField(lx, 'M', ref pos, st);
                    d = GetShortDateField(lx, 'd', ref pos, st);
                    lx.pos = pos;
                    lx.ch = (pos < lx.input.Length) ? lx.input[pos] : (char)0;
                }
                return new DateTime(y, m, d);
            }
            catch (Exception)
            {
                throw new DBException("22007", /*e.Message*/Diag(lx, st)).Mix();
            }
        }
        string Diag(Scanner lx, int st)
        {
            var n = lx.input.Length - st;
            if (n > 20)
                n = 20;
            return lx.String(st, n);
        }
        /// <summary>
        /// Get a Timestamp from the string
        /// </summary>
        /// <returns>DateTime</returns>
        DateTime GetTimestamp(Scanner lx, int st)
        {
            DateTime d = GetDate(lx, st);
            if (lx.ch == 0)
                return d;
            if (lx.ch != ' ' && lx.ch != 'T')
                throw new DBException("22008", Diag(lx, st)).Mix();
            lx.Advance();
            TimeSpan r = GetTime(lx, st);
            return d + r;
        }
        /// <summary>
        /// Get the time part from the string (ISO 8601)
        /// </summary>
        /// <returns>a TimeSpan</returns>
        TimeSpan GetTime(Scanner lx, int st)
        {
            int h = GetHour(lx, st);
            int m = 0;
            int s = 0;
            int f = 0;
            if (lx.ch == ':' || System.Char.IsDigit(lx.ch))
            {
                if (lx.ch == ':')
                    lx.Advance();
                m = GetMinutes(lx, st);
                if (lx.ch == ':' || System.Char.IsDigit(lx.ch))
                {
                    if (lx.ch == ':')
                        lx.Advance();
                    s = GetSeconds(lx, st);
                    if (lx.ch == '.')
                    {
                        lx.Advance();
                        var nst = lx.pos;
                        f = GetUnsigned(lx, st);
                        int n = lx.pos - nst;
                        if (n > 6)
                            throw new DBException("22008", Diag(lx, st)).Mix();
                        while (n < 7)
                        {
                            f *= 10;
                            n++;
                        }
                    }
                }
            }
            TimeSpan r = new TimeSpan(h, m, s);
            if (f != 0)
                r += TimeSpan.FromTicks(f);
            return r + GetTimeZone(lx, st);
        }
        TimeSpan GetTimeZone(Scanner lx, int st)
        {
            if (lx.ch == 'Z')
            {
                lx.Advance();
                return TimeSpan.Zero;
            }
            var s = lx.ch;
            if (s != '+' && s != '-')
                return TimeSpan.Zero;
            lx.Advance();
            var z = GetTime(lx, st);
            return (s == '+') ? z : -z;
        }
        /// <summary>
        /// ShortDatePattern: d.M.yy for example means 1 or 2 digits for d and M.
        /// So we need to identify and count delimiters to get the field
        /// </summary>
        /// <param name="f"></param>
        /// <param name="delim">delimiter used in pattern</param>
        /// <param name="delimsBefore">Number of delimiters before the desired field</param>
        /// <param name="len"></param>
        void GetShortDatePattern(char f, ref char delim, out int delimsBefore, out int len)
        {
            var pat = CultureInfo.CurrentCulture.DateTimeFormat.ShortDatePattern;
            var found = false;
            delimsBefore = 0;
            int off = 0;
            for (; off < pat.Length; off++)
            {
                var c = pat[off];
                if (delim == (char)0 && c != 'y' && c != 'M' && c != 'd')
                    delim = c;
                if (c == delim)
                    delimsBefore++;
                if (pat[off] == f)
                {
                    found = true;
                    break;
                }
            }
            if (!found)
                throw new DBException("22007", "Bad Pattern " + pat);
            for (len = 0; off + len < pat.Length && pat[off + len] == f; len++)
                ;
        }
        int GetShortDateField(Scanner lx, char f, ref int pos, int st)
        {
            var p = lx.pos;
            var ch = lx.ch;
            var delim = (char)0;
            GetShortDatePattern(f, ref delim, out int dbef, out int len);
            var r = GetNDigits(lx, delim, dbef, len, st);
            if (f == 'y' && len == 2)
            {
                if (r >= 50)
                    r += 1900;
                else
                    r += 2000;
            }
            if (pos < lx.pos)
                pos = lx.pos;
            lx.pos = p;
            lx.ch = ch;
            return r;
        }
        /// <summary>
        /// Get N (or, if N==1, 2) digits for months, days, hours, minutues, seconds
        /// </summary>
        /// <returns>an int</returns>
        int GetNDigits(Scanner lx, char delim, int dbef, int n, int st)
        {
            for (; lx.ch != (char)0 && dbef > 0; dbef--)
            {
                while (lx.ch != (char)0 && lx.ch != delim)
                    lx.Advance();
                if (lx.ch != (char)0)
                    lx.Advance();
            }
            if (lx.ch == (char)0)
                throw new DBException("22008", Diag(lx, st)).ISO();
            var s = lx.pos;
            for (int i = 0; i < n; i++)
            {
                if (!System.Char.IsDigit(lx.ch))
                    throw new DBException("22008", Diag(lx, st)).ISO();
                lx.Advance();
            }
            if (n == 1 && System.Char.IsDigit(lx.ch))
            {
                n++; lx.Advance();
            }
            return int.Parse(new string(lx.input, s, n));
        }
        /// <summary>
        /// get an hour as 2 digits
        /// </summary>
        /// <returns>an int</returns>
        int GetHour(Scanner lx, int st)
        {
            int h = GetNDigits(lx, ':', 0, 2, st);
            if (h < 0 || h > 23)
                throw new DBException("22008", Diag(lx, st)).ISO();
            return h;
        }
        /// <summary>
        /// get minutes as 2 digits
        /// </summary>
        /// <returns>an int</returns>
        int GetMinutes(Scanner lx, int st)
        {
            int m = GetNDigits(lx, ':', 0, 2, st);
            if (m < 0 || m > 59)
                throw new DBException("22008", Diag(lx, st)).ISO();
            return m;
        }
        /// <summary>
        /// get seconds as 2 digits
        /// </summary>
        /// <returns>an int</returns>
        int GetSeconds(Scanner lx, int st)
        {
            int m = GetNDigits(lx, '.', 0, 2, st);
            if (m < 0 || m > 59)
                throw new DBException("22008", Diag(lx, st)).ISO();
            return m;
        }
        /// <summary>
        /// get the fractional seconds part
        /// </summary>
        /// <returns></returns>
        int GetUnsigned(Scanner lx, int st)
        {
            while (char.IsWhiteSpace(lx.ch))
                lx.Advance();
            int s = lx.pos;
            while (char.IsDigit(lx.ch))
                lx.Advance();
            return int.Parse(new string(lx.input, s, lx.pos - s));
        }
        /// <summary>
        /// Coerce a given value to this type, bomb if it isn't possible
        /// </summary>
        /// <param name="v"></param>
        /// <returns></returns>
        internal virtual TypedValue Coerce(Context cx,TypedValue v)
        {
            if (v==TNull.Value)
                return v;
            if (v is TArray ta && ta.Length == 1 && CanTakeValueOf(ta.dataType))
                return Coerce(cx, ta[0]);
            for (var b = constraints?.First(); b != null; b = b.Next())
                if (cx.obs[b.key()].Eval(null) != TBool.True)
                    throw new DBException("22211");
            if (kind == Sqlx.UNION)
                for (var b = unionOf.First(); b != null; b = b.Next())
                {
                    var du = b.key();
                    if (du.HasValue(cx,v))
                        return v;
                }
            if (v == null || v.IsNull)
                return v;
            if (abbrev != "" && v.dataType.kind == Sqlx.CHAR && kind != Sqlx.CHAR)
                v = Parse(new Scanner(-1,v.ToString().ToCharArray(), 0));
            if (CompareTo(v.dataType) == 0)
                return v;
            var vk = Equivalent(v.dataType.kind);
            if (Equivalent(kind)!=Sqlx.ROW && (vk == Sqlx.ROW || vk==Sqlx.TABLE) && v is TRow rw && rw.Length == 1)
            {
                var b = rw.dataType.representation.First();
                return b.value().Coerce(cx,rw.values[b.key()]);
            }
            if (iri == null || v.dataType.iri == iri)
                switch (Equivalent(kind))
                {
                    case Sqlx.INTEGER:
                        {
                            if (vk == Sqlx.INTEGER)
                            {
                                if (prec != 0)
                                {
                                    Integer iv;
                                    if (v.Val() is long)
                                        iv = new Integer((long)v.Val());
                                    else
                                        iv = v.Val() as Integer;
                                    var limit = Integer.Pow10(prec);
                                    if (iv >= limit || iv <= -limit)
                                        throw new DBException("22003").ISO()
                                            .AddType(this).AddValue(v);
                                    return new TInteger(this, iv);
                                }
                                if (v.Val() is long)
                                    return new TInt(this, v.ToLong());
                                return new TInteger(this, v.ToInteger());
                            }
                            if (vk == Sqlx.NUMERIC)
                            {
                                var a = v.Val() as Common.Numeric;
                                var m = a.mantissa;
                                var s = a.scale;
                                int r = 0;
                                while (s > 0)
                                {
                                    m = m.Quotient(10, ref r);
                                    s--;
                                }
                                while (s < 0)
                                {
                                    m = a.mantissa.Times(10);
                                    s++;
                                }
                                a = new Numeric(m, s);
                                if (prec != 0)
                                {
                                    var limit = Integer.Pow10(prec);
                                    if (a.mantissa >= limit || a.mantissa <= -limit)
                                        throw new DBException("22003").ISO()
                                            .AddType(this).Add(Sqlx.VALUE, v);
                                }
                                return new TInteger(this, a.mantissa);
                            }
                            if (vk == Sqlx.REAL)
                            {
                                var ii = v.ToLong().Value;
                                if (prec != 0)
                                {
                                    var iv = new Integer(ii);
                                    var limit = Integer.Pow10(prec);
                                    if (iv > limit || iv < -limit)
                                        throw new DBException("22003").ISO()
                                             .AddType(this).AddValue(v);
                                }
                                return new TInt(this, ii);
                            }
                            if (vk == Sqlx.CHAR)
                                return new TInt(Integer.Parse(v.ToString()));
                        }
                        break;
                    case Sqlx.NUMERIC:
                        {
                            Common.Numeric a;
                            var ov = v.Val();
                            if (vk == Sqlx.NUMERIC)
                                a = (Numeric)ov;
                            else if (ov == null)
                                a = null;
                            else if (ov is long?)
                                a = new Numeric(v.ToLong().Value);
                            else if (v.Val() is Integer)
                                a = new Common.Numeric((Integer)v.Val());
                            else if (v.Val() is double)
                                a = new Common.Numeric(v.ToDouble());
                            else
                                break;
                            if (scale != 0)
                            {
                                if ((!a.mantissa.IsZero()) && a.scale > scale)
                                    a = a.Round(scale);
                                int r = 0;
                                var m = a.mantissa;
                                var s = a.scale;
                                while (s > scale)
                                {
                                    m = m.Quotient(10, ref r);
                                    s--;
                                }
                                while (s < scale)
                                {
                                    m = m.Times(10);
                                    s++;
                                }
                                a = new Numeric(m, s);
                            }
                            if (prec != 0)
                            {
                                var limit = Integer.Pow10(prec);
                                if (a.mantissa > limit || a.mantissa < -limit)
                                    throw new DBException("22003").ISO()
                                         .AddType(this).AddValue(v);
                            }
                            return new TNumeric(this, a);
                        }
                    case Sqlx.REAL:
                        {
                            var r = v.ToDouble();
                            if (prec == 0)
                                return new TReal(this, r);
                            decimal d = new decimal(r);
                            d = Math.Round(d, scale);
                            bool sg = d < 0;
                            if (sg)
                                d = -d;
                            decimal m = 1.0M;
                            for (int j = 0; j < prec - scale; j++)
                                m = m * 10.0M;
                            if (d > m)
                                break;
                            if (sg)
                                d = -d;
                            return new TReal(this, (double)d);
                        }
                    case Sqlx.DATE:
                        switch (vk)
                        {
                            case Sqlx.DATE:
                                return v;
                            case Sqlx.CHAR:
                                return new TDateTime(this, DateTime.Parse(v.ToString(),
                                    v.dataType.culture));
                        }
                        if (v.Val() is DateTime)
                            return new TDateTime(this, (DateTime)v.Val());
                        if (v.Val() is long)
                            return new TDateTime(this, new DateTime(v.ToLong().Value));
                        break;
                    case Sqlx.TIME:
                        switch (vk)
                        {
                            case Sqlx.TIME:
                                return v;
                            case Sqlx.CHAR:
                                return new TTimeSpan(this, TimeSpan.Parse(v.ToString(),
                                    v.dataType.culture));
                        }
                        break;
                    case Sqlx.TIMESTAMP:
                        switch (vk)
                        {
                            case Sqlx.TIMESTAMP: return v;
                            case Sqlx.DATE:
                                return new TDateTime(this, ((Date)v.Val()).date);
                            case Sqlx.CHAR:
                                return new TDateTime(this, DateTime.Parse(v.ToString(),
                                    v.dataType.culture));
                        }
                        if (v.Val() is long)
                            return new TDateTime(this, new DateTime(v.ToLong().Value));
                        break;
                    case Sqlx.INTERVAL:
                        if (v.Val() is Interval)
                            return new TInterval(this, v.Val() as Interval);
                        break;
                    case Sqlx.CHAR:
                        {
                            var vt = v.dataType;
                            string str;
                            switch (vt.kind)
                            {
                                case Sqlx.TIMESTAMP: str = ((DateTime)(v.Val())).ToString(culture); break;
                                case Sqlx.DATE: str = ((Date)v.Val()).date.ToString(culture); break;
                                case Sqlx.CHAR: str = (string)v.Val(); break;
                                case Sqlx.CHECK: str = ((Rvv)v.Val())?.ToString()??""; break;
                                default: str = v.ToString(); break;
                            }
                            if (prec != 0 && str.Length > prec)
                                throw new DBException("22001", "CHAR(" + prec + ")", "CHAR(" + str.Length + ")").ISO()
                                                    .AddType(this).AddValue(vt);
                            return new TChar(this, str);
                        }
                    case Sqlx.PERIOD:
                        {
                            var pd = v.Val() as Period;
                            var et = cx._Dom(elType);
                            return new TPeriod(this, new Period(et.Coerce(cx,pd.start), 
                                et.Coerce(cx,pd.end)));
                        }
                    case Sqlx.DOCUMENT:
                        {
                            switch (vk)
                            {
                                case Sqlx.CHAR:
                                    {
                                        var vs = v.ToString();
                                        if (vs[0] == '{')
                                            return new TDocument(vs);
                                        break;
                                    }
                                case Sqlx.BLOB:
                                    {
                                        var i = 0;
                                        return new TDocument((byte[])v.Val(), ref i);
                                    }
                            }
                            return v;
                        }
                    case Sqlx.CONTENT: return v;
                    case Sqlx.PASSWORD: return v;
                    case Sqlx.DOCARRAY: goto case Sqlx.DOCUMENT;
                    case Sqlx.ROW:
                        {
                            var vs = CTree<long, TypedValue>.Empty;
                            var vb = v.dataType.rowType.First();
                            var d = v.dataType.display;
                            if (d == 0)
                                d = int.MaxValue;
                            for (var b = rowType.First(); b != null && b.key() < d; b = b.Next(),vb=vb.Next())
                            {
                                if (vb == null)
                                    goto bad;
                                var p = b.value();
                                var dt = cx._Dom(representation[p]);
                                vs += (p, dt.Coerce(cx,v[vb.value()]));
                            }
                            if (vb != null)
                                goto bad;
                            return new TRow(this, vs);
                        }
                    case Sqlx.VALUE:
                    case Sqlx.NULL:
                        return v;
                }
            bad:  throw new DBException("22005", this, v.ToString()).ISO();
        }
        /// <summary>
        ///  for accepting Json values
        /// </summary>
        /// <param name="ro"></param>
        /// <param name="ob"></param>
        /// <returns></returns>
        internal TypedValue Coerce(Context cx,object ob)
        {
            var ro = cx.db.role;
            if (ob==null)
                return defaultValue;
            if (abbrev != "" && ob is string && Equivalent(kind) != Sqlx.CHAR)
                return Parse(new Scanner(-1, ((string)ob).ToCharArray(), 0));
            switch (Equivalent(kind))
            {
                case Sqlx.UNION:
                    for (var b = unionOf.First(); b != null; b = b.Next())
                    {
                        var du = b.key();
                        if (du.Coerce(cx, ob) is TypedValue t0)
                            return t0;
                    }
                    break;
                case Sqlx.INTEGER:
                    if (ob is long)
                        return new TInt(this, (long)ob);
                    return new TInt(this, (int)ob);
                case Sqlx.NUMERIC:
                    {
                        Numeric nm = null;
                        if (ob is long)
                            nm = new Numeric((long)ob);
                        if (ob is double)
                            nm = new Numeric((double)ob);
                        if (ob is decimal)
                            nm = new Numeric((decimal)ob);
                        if (ob is int)
                            nm = new Numeric((int)ob);
                        return new TNumeric(this, nm);
                    }
                case Sqlx.REAL:
                    if (ob is decimal)
                        return new TReal(this, (double)(decimal)ob);
                    if (ob is float)
                        return new TReal(this, (float)ob);
                    return new TReal(this, (double)ob);
                case Sqlx.DATE:
                    return new TDateTime(this, (DateTime)ob);
                case Sqlx.TIME:
                    return new TTimeSpan(this, (TimeSpan)ob);
                case Sqlx.TIMESTAMP:
                    if (ob is DateTime)
                        return new TDateTime(this, (DateTime)ob);
                    if (ob is Date)
                        return new TDateTime(this, ((Date)ob).date);
                    if (ob is string)
                        return new TDateTime(this,
                            DateTime.Parse((string)ob, culture));
                    if (ob is long)
                        return new TDateTime(this, new DateTime((long)ob));
                    break;
                case Sqlx.INTERVAL:
                    if (ob is Interval)
                        return new TInterval(this, (Interval)ob);
                    break;
                case Sqlx.CHAR:
                    {
                        string str = "";
                        if (ob is DateTime)
                            str = ((DateTime)ob).ToString(culture);
                        if (ob is Date)
                            str = ((Date)ob).ToString();
                        if (ob is string)
                            str = (string)ob;
                        if (prec != 0 && str.Length > prec)
                            throw new DBException("22001", "CHAR(" + prec + ")", "CHAR(" + str.Length + ")").ISO()
                                                .AddType(this);
                        return new TChar(this, str);
                    }
                case Sqlx.PERIOD:
                    {
                        var pd = ob as Period;
                        var et = cx._Dom(elType);
                        return new TPeriod(this, new Period(et.Coerce(cx, pd.start),
                            et.Coerce(cx, pd.end)));
                    }
                case Sqlx.DOCUMENT:
                    {
                        if (ob is string vs && vs[0] == '{')
                            return new TDocument(vs);
                        int i = 0;
                        if (ob is byte[] bs)
                            return new TDocument(bs, ref i);
                        break;
                    }
                case Sqlx.ROW:
                    if (ob is Document){
                        var d = (Document)ob;
                        var vs = CTree<long, TypedValue>.Empty;
                        for (var b = rowType.First(); b != null; b = b.Next())
                        {
                            var p = b.value();
                            var cn = (cx.obs[p] is SqlValue sv) ? sv.name
                                : (ro.infos[p] is ObInfo ci) ? ci.name
                                : null;
                            if (cn == null || cn == "")
                                cn = "Col" + b.key();
                            var di = cn.IndexOf(".");
                            if (di > 0)
                                cn = cn.Substring(di+1);
                            var an = (cx.obs[p] is SqlValue sv0) ? sv0.alias
                                : (ro.infos[p] is ObInfo ci0) ? ci0.alias
                                : null;
                            var co = (cn != "" && cn != null && d.Contains(cn)) ? d[cn]
                                : (an != "" && an != null && d.Contains(an)) ? d[an]
                                : TNull.Value;
                            var cd = cx._Dom(representation[p]);
                            if (co != null && co != TNull.Value)
                            {
                                var v = cd.Coerce(cx, co);
                                vs += (p, v);
                            }
                        }
                        var kb = aggs.First();
                        foreach (var f in d.fields)
                        {
                            if (f.Key == "$check")
                            {
                                vs += (LastChange, new TInt((long)f.Value));
                                vs += (Defpos, new TInt((long)d["$pos"]));
                            }
                            if (f.Key == "$provenance")
                                vs += (Provenance, new TChar((string)f.Value));
                            if (f.Key == "%classification")
                                vs += (Classification,
                                    TLevel.New(Level.Parse((string)f.Value)));
                            if (f.Key.StartsWith("$#"))
                            {
                                var sf = (SqlFunction)cx.obs[kb.key()]; kb = kb.Next();
                                var dm = cx._Dom(sf);
                                var key = TRow.Empty;
                                if (cx.groupCols[this] is Domain gc)
                                {
                                    var ks = CTree<long, TypedValue>.Empty;
                                    for (var g = gc.rowType.First(); g != null; g = g.Next())
                                    {
                                        var gp = g.value();
                                        var gs = cx.obs[gp];
                                        var nm = gs.alias ?? gs.name ?? "";
                                        var gd = cx._Dom(gs);
                                        ks += (gp, gd.Coerce(cx, d[nm]));
                                    }
                                    key = new TRow(gc, ks);
                                    cx.values += key.values;
                                }
                                var fd = (Document)f.Value;
                                var ra = fd.fields.ToArray();
                                var fc = cx.funcs[sf.from]?[key]?[sf.defpos] ?? sf.StartCounter(cx,key);
                                fc.count += int.Parse(ra[0].Key);
                                switch (sf.kind)
                                {
                                    case Sqlx.STDDEV_POP:
                                        fc.acc1 += (double)ra[1].Value;
                                        goto case Sqlx.SUM;
                                    case Sqlx.AVG:
                                    case Sqlx.SUM:
                                        switch (dm.kind)
                                        {
                                            case Sqlx.INTEGER:
                                                {
                                                    var tv = dm.Coerce(cx, ra[0].Value);
                                                    var iv =
                                                    (tv is TInt) ? new Integer(tv.ToLong().Value)
                                                        : tv.ToInteger();
                                                    if (fc.sumInteger == null)
                                                        fc.sumInteger = iv;
                                                    else
                                                        fc.sumInteger += iv;
                                                    fc.sumType = Int;
                                                }
                                                break;
                                            case Sqlx.NUMERIC:
                                                {
                                                    var dv = (Numeric)dm.Coerce(cx, ra[0].Value).Val();
                                                    if (fc.sumType != Numeric)
                                                        fc.sumDecimal = dv;
                                                    else
                                                        fc.sumDecimal += dv;
                                                    fc.sumType = Numeric;
                                                }
                                                break;
                                            case Sqlx.REAL:
                                                fc.sum1 += dm.Coerce(cx, ra[0].Value).ToDouble();
                                                fc.sumType = Real;
                                                break;
                                        }
                                        break;
                                    case Sqlx.FIRST:
                                        if (fc.acc == null)
                                            fc.acc = dm.Coerce(cx, ra[0].Value);
                                        break;
                                    case Sqlx.LAST:
                                        fc.acc = dm.Coerce(cx, ra[0].Value);
                                        break;
                                    case Sqlx.MAX:
                                        {
                                            var m = dm.Coerce(cx, ra[0].Value);
                                            if (m.CompareTo(fc.acc) > 0)
                                                fc.acc = m;
                                            break;
                                        }
                                    case Sqlx.MIN:
                                        {
                                            var m = dm.Coerce(cx, ra[0].Value);
                                            if (fc.acc == null || m.CompareTo(fc.acc) < 0)
                                                fc.acc = m;
                                            break;
                                        }
                                    case Sqlx.COLLECT:
                                    case Sqlx.DISTINCT:
                                    case Sqlx.INTERSECTION:
                                    case Sqlx.FUSION:
                                        {
                                            var ma = (Document)ra[0].Value;
                                            var mm = ma.fields.ToArray();
                                            for (int j = 0; j < mm.Length; j++)
                                                fc.mset.Add(dm.Coerce(cx, mm[j].Value));
                                        }
                                        break;
                                    case Sqlx.SOME:
                                    case Sqlx.ANY:
                                        fc.bval = fc.bval || (bool)ra[0].Value;
                                        break;
                                    case Sqlx.EVERY:
                                        fc.bval = fc.bval && (bool)ra[0].Value;
                                        break;
                                    case Sqlx.ROW_NUMBER:
                                        fc.row = (long)ra[0].Value;
                                        break;
                                }
                                var t1 = cx.funcs[sf.from] ?? BTree<TRow, BTree<long, Register>>.Empty;
                                var t2 = t1[key] ?? BTree<long, Register>.Empty;
                                t2 += (sf.defpos, fc);
                                t1 += (key, t2);
                                cx.funcs += (sf.from, t1);
                                cx.values -= sf.defpos;
                                var v = sf.Eval(cx);
                                cx.values += (sf.defpos, v);
                                vs += (sf.defpos, v);
                            }
                        }
                        var r = new TRow(this, vs);
                        if (d.Contains("$check"))
                            r += (Common.Rvv.RVV, new TRvv(cx, vs));
                        return r;
                    }
                    break;
                case Sqlx.TABLE:
                    if (ob is DocArray da)
                    {
                        var dr = (Domain)Relocate(cx.GetUid()) + (Kind, Sqlx.ROW);
                        cx.groupCols += (dr, cx.groupCols[this]);
                        var va = BList<TypedValue>.Empty;
                        foreach (var o in da.items)
                            va += dr.Coerce(cx, o);
                        return new TArray(dr, va);
                    }
                    break;
                case Sqlx.ARRAY:
                    if (ob is DocArray db)
                    {
                        var va = BList<TypedValue>.Empty;
                        foreach (var o in db.items)
                            va += elType.Coerce(cx, o);
                        return new TArray(elType, va);
                    }
                    break;
                case Sqlx.MULTISET:
                    if (ob is DocArray dc)
                    {
                        long n = 0;
                        var va = CTree<TypedValue,long>.Empty;
                        foreach (var o in dc.items)
                        {
                            var v = elType.Coerce(cx, o);
                            var k = va.Contains(v)?va[v] : 0L;
                            va += (v, k);
                            n++;
                        }
                        return new TMultiset(elType, va, n);
                    }
                    break;
            }
            throw new DBException("22005", this, ob.ToString()).ISO();
        }
        /// <summary>
        /// The System.Type corresponding to a SqlDataType
        /// </summary>
        public Type SystemType
        {
            get
            {
                switch (Equivalent(kind))
                {
                    case Sqlx.ONLY: return (this as UDType)?.super?.SystemType;
                    case Sqlx.NULL: return typeof(DBNull);
                    case Sqlx.INTEGER: return typeof(long);
                    case Sqlx.NUMERIC: return typeof(decimal);
                    case Sqlx.BLOB: return typeof(byte[]);
                    case Sqlx.NCHAR: goto case Sqlx.CHAR;
                    case Sqlx.CLOB: goto case Sqlx.CHAR;
                    case Sqlx.NCLOB: goto case Sqlx.CHAR;
                    case Sqlx.REAL: return typeof(double);
                    case Sqlx.CHAR: return typeof(string);
                    case Sqlx.PASSWORD: goto case Sqlx.CHAR;
                    case Sqlx.DATE: return typeof(Date);
                    case Sqlx.TIME: return typeof(TimeSpan);
                    case Sqlx.INTERVAL: return typeof(Interval);
                    case Sqlx.BOOLEAN: return typeof(bool);
                    case Sqlx.TIMESTAMP: return typeof(DateTime);
                    //#if EMBEDDED
                    case Sqlx.DOCUMENT: return typeof(Document);
                        //#else
                        //                    case Sqlx.DOCUMENT: return typeof(byte[]);
                        //#endif
                }
                return typeof(object);
            }
        }
        /// <summary>
        /// Select a predefined obs type
        /// </summary>
        /// <param name="t">the token</param>
        /// <returns>the corresponding predefined type</returns>
        public static Domain Predefined(Sqlx t)
        {
            switch (Equivalent(t))
            {
                case Sqlx.BLOB: return Blob;
                case Sqlx.BLOBLITERAL: return Blob;
                case Sqlx.BOOLEAN: return Bool;
                case Sqlx.BOOLEANLITERAL: return Bool;
                case Sqlx.CHAR: return Char;
                case Sqlx.CHARLITERAL: return Char;
                case Sqlx.DATE: return Date;
                case Sqlx.DOCARRAY: return Document;
                case Sqlx.DOCUMENT: return Document;
                case Sqlx.DOCUMENTLITERAL: return Document;
                case Sqlx.INTEGER: return Int;
                case Sqlx.INTEGERLITERAL: return Int;
                case Sqlx.INTERVAL: return Interval;
                case Sqlx.NULL: return Null;
                case Sqlx.NUMERIC: return Numeric;
                case Sqlx.NUMERICLITERAL: return Numeric;
                case Sqlx.PASSWORD: return Password;
                case Sqlx.POSITION: return Int;
                case Sqlx.REAL: return Real;
                case Sqlx.REALLITERAL: return Real;
                case Sqlx.TIME: return Timespan;
                case Sqlx.TIMESTAMP: return Timestamp;
            }
            throw new DBException("42119",t,"CURRENT");
        }
        /// <summary>
        /// Validator
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public bool Check(string s)
        {
            if (charSet == CharSet.UCS)
                return true;
            try
            {
                byte[] x = Encoding.UTF8.GetBytes(s); // throws exception if not even UCS
                int n = s.Length;
                if (charSet <= CharSet.ISO8BIT && x.Length != n)
                    return false;
                for (int j = 0; j < n; j++)
                {
                    if (charSet <= CharSet.LATIN1 && x[j] > 128)
                        return false;
                    if (charSet <= CharSet.GRAPHIC_IRV && x[j] < 32)
                        return false;
                    byte b = x[j];
                    if (charSet <= CharSet.SQL_IDENTIFIER &&
                        (b == 0x21 || b == 0x23 || b == 0x24 || b == 0x40 || b == 0x5c || b == 0x60 || b == 0x7e))
                        return false;
                }
            }
            catch (Exception)
            {
                return false;
            }
            return true;
        }
        /// <summary>
        /// Set up a CultureInfo given a collation or culture name.
        /// We support the names specified in the SQL2003 standard, together
        /// with the cultures supported by .NET
        /// </summary>
        /// <param name="n">The name of a collation or culture</param>
        /// <returns></returns>
        public static CultureInfo GetCulture(string n)
        {
            if (n == null)
                return null;
            n = n.ToLower();
            try
            {
                switch (n)
                {
                    case "ucs_binary": return null;
                    case "sql_character": return null;
                    case "graphic_irv": return null;
                    case "sql_text": return null;
                    case "sql_identifier": return null;
                    case "latin1": return CultureInfo.InvariantCulture;
                    case "iso8bit": return CultureInfo.InvariantCulture;
                    case "unicode": return CultureInfo.InvariantCulture;
                    default: return new CultureInfo(n);
                }
            }
            catch (Exception e)
            {
                throw new DBException("2H000", e.Message).ISO();
            }
        }
        internal override TypedValue Eval(Context cx)
        {
            var v = base.Eval(cx);
            if (v != null && !v.IsNull)
                return v;
            if (rowType != CList<long>.Empty)
            {
                var vs = CTree<long, TypedValue>.Empty;
                for (var b = rowType.First(); b != null; b = b.Next())
                {
                    var p = b.value();
                    vs += (p, cx.obs[p].Eval(cx));
                }
                return new TRow(this, vs);
            }
            return defaultValue;
        }
        /// <summary>
        /// Evaluate a binary operation 
        /// </summary>
        /// <param name="a">The first object</param>
        /// <param name="op">The binary operation</param>
        /// <param name="b">The second object</param>
        /// <returns>The evaluated object</returns>
        public TypedValue Eval(long lp,Context cx,TypedValue a, Sqlx op, TypedValue b) // op is + - * / so a and b should be compatible arithmetic types
        {
            if (sensitive)
                return new TSensitive(this, Eval(lp,cx,a, op, b));
            if (op == Sqlx.NO)
                return Coerce(cx,a);
            if (a == null || a.IsNull || b == null || b.IsNull)
                return defaultValue;
            if (a is TUnion)
                a = ((TUnion)a).LimitToValue(cx,lp); // a coercion possibly
            if (b is TUnion)
                b = ((TUnion)b).LimitToValue(cx,lp);
            var knd = Equivalent(kind);
            var ak = Equivalent(a.dataType.kind);
            var bk = Equivalent(b.dataType.kind);
            if (knd == Sqlx.UNION)
            {
                if (ak == bk)
                    knd = ak;
                else if (ak == Sqlx.REAL || bk == Sqlx.REAL)
                    knd = Sqlx.REAL;
                else if (ak != Sqlx.INTEGER || bk != Sqlx.INTEGER)
                    knd = Sqlx.NUMERIC;
            }
            switch (knd)
            {
                case Sqlx.INTEGER:
                    if (ak == Sqlx.NUMERIC)
                        a = new TInteger(a.ToInteger());
                    if (bk == Sqlx.INTERVAL && kind == Sqlx.TIMES)
                        return Eval(lp,cx,b, op, a);
                    if (bk == Sqlx.NUMERIC)
                        b = new TInteger(b.ToInteger());
                    if (ak == Sqlx.INTEGER)
                    {
                        if (a.Val() is long)
                        {
                            if ((bk == Sqlx.INTEGER || bk == Sqlx.UNION) && b.Val() is long)
                            {
                                long aa = a.ToLong().Value, bb = b.ToLong().Value;
                                switch (op)
                                {
                                    case Sqlx.PLUS:
                                        if (aa == 0)
                                            return b;
                                        if (aa > 0 && (bb <= 0 || aa < long.MaxValue - bb))
                                            return new TInt(this, aa + bb);
                                        else if (aa < 0 && (bb >= 0 || aa > long.MinValue - bb))
                                            return new TInt(this, aa + bb);
                                        return new TInteger(this, new Integer(aa) + new Integer(bb));
                                    case Sqlx.MINUS:
                                        if (bb == 0)
                                            return a;
                                        if (bb > 0 && (aa >= 0 || aa > long.MinValue + bb))
                                            return new TInt(this, aa - bb);
                                        else if (bb < 0 && (aa >= 0 || aa < long.MaxValue + bb))
                                            return new TInt(this, aa - bb);
                                        return new TInteger(this, new Integer(aa) - new Integer(bb));
                                    case Sqlx.TIMES:
                                        if (aa < int.MaxValue && aa > int.MinValue && bb < int.MaxValue && bb > int.MinValue)
                                            return new TInt(this, aa * bb);
                                        return new TInteger(this, new Integer(aa) * new Integer(bb));
                                    case Sqlx.DIVIDE: return new TInt(this, aa / bb);
                                }
                            }
                            else if (b.Val() is Integer)
                                return IntegerOps(this, new Integer(a.ToLong().Value), op, (Integer)b.Val());
                        }
                        else if (a.Val() is Integer)
                        {
                            if (b.Val() is long)
                                return IntegerOps(this, (Integer)a.Val(), op, new Integer(b.ToLong().Value));
                            else if (b.Val() is Integer)
                                return IntegerOps(this, (Integer)a.Val(), op, (Integer)b.Val());
                        }
                    }
                    break;
                case Sqlx.REAL:
                    return new TReal(this, DoubleOps(a.ToDouble(), op, b.ToDouble()));
                case Sqlx.NUMERIC:
                    if (a.dataType.Constrain(cx,lp,Int) != null)
                        a = new TNumeric(new Numeric(a.ToInteger(), 0));
                    if (b.dataType.Constrain(cx,lp,Int) != null)
                        b = new TNumeric(new Numeric(b.ToInteger(), 0));
                    if (a is TNumeric && b is TNumeric)
                        return new TNumeric(DecimalOps(((TNumeric)a).value, op, ((TNumeric)b).value));
                    var ca = a.ToDouble();
                    var cb = b.ToDouble();
                    return Coerce(cx,new TReal(this, DoubleOps(ca, op, cb)));
                case Sqlx.TIME:
                case Sqlx.TIMESTAMP:
                case Sqlx.DATE:
                    {
                        var ta = (DateTime)a.Val();
                        switch (bk)
                        {
                            case Sqlx.INTERVAL:
                                {
                                    var ib = (Interval)b.Val();
                                    switch (op)
                                    {
                                        case Sqlx.PLUS: return new TDateTime(this, ta.AddYears(ib.years).AddMonths(ib.months).AddTicks(ib.ticks));
                                        case Sqlx.MINUS: return new TDateTime(this, ta.AddYears(-ib.years).AddMonths(ib.months).AddTicks(-ib.ticks));
                                    }
                                    break;
                                }
                            case Sqlx.TIME:
                            case Sqlx.TIMESTAMP:
                            case Sqlx.DATE:
                                {
                                    if (b.IsNull)
                                        return TNull.Value;
                                    if (op == Sqlx.MINUS)
                                        return DateTimeDifference(ta, (DateTime)b.Val());
                                    break;
                                }
                        }
                        throw new DBException("42161", "date operation");
                    }
                case Sqlx.INTERVAL:
                    {
                        var ia = (Interval)a.Val();
                         switch (bk)
                        {
                            case Sqlx.DATE:
                                return Eval(lp,cx,b, op, a);
                            case Sqlx.INTEGER:
                                var bi = b.ToInt().Value;
                                if (ia.yearmonth)
                                {
                                    var m = ia.years * 12 + ia.months;
                                    var y = 0;
                                    switch (kind)
                                    {
                                        case Sqlx.TIMES: m = m * bi; break;
                                        case Sqlx.DIVIDE: m = m / bi; break;
                                    }
                                    if (start == Sqlx.YEAR)
                                    {
                                        y = m / 12;
                                        if (end == Sqlx.MONTH)
                                            m = m - 12 * (m / 12);
                                    }
                                    return new TInterval(this, new Interval(y,m));
                                }
                                break;
                            case Sqlx.INTERVAL:
                                var ib = (Interval)b.Val();
                                Interval ic;
                                if (ia.yearmonth != ib.yearmonth)
                                    break;
                                if (ia.yearmonth)
                                    switch (kind)
                                    {
                                        case Sqlx.PLUS: ic = new Interval(ia.years + ib.years, ia.months + ib.months); break;
                                        case Sqlx.MINUS: ic = new Interval(ia.years - ib.years, ia.months - ib.months); break;
                                        default: throw new PEException("PE56");
                                    }
                                else
                                    switch (kind)
                                    {
                                        case Sqlx.PLUS: ic = new Interval(ia.ticks - ib.ticks); break;
                                        case Sqlx.MINUS: ic = new Interval(ia.ticks - ib.ticks); break;
                                        default: throw new PEException("PE56");
                                    }
                                return new TInterval(this, ic);
                        }
                        throw new DBException("42161", "date operation");
                    }
                case Sqlx.RDFTYPE:
                    return Coerce(cx,cx._Dom(elType).Eval(lp,cx,a, op, b));
            }
            throw new DBException("22005", kind, a).ISO();
        }
        /// <summary>
        /// MaxLong bound for knowing if an Integer will fit into a long
        /// </summary>
        static Integer MaxLong = new Integer(long.MaxValue);
        /// <summary>
        /// MinLong bound for knowing if an Integer will fit into a long
        /// </summary>
        static Integer MinLong = new Integer(long.MinValue);
        /// <summary>
        /// Integer operations
        /// </summary>
        /// <param name="a">The left Integer operand</param>
        /// <param name="op">The operator</param>
        /// <param name="b">The right Integer operand</param>
        /// <returns>The Integer result</returns>
        static TypedValue IntegerOps(Domain tp, Integer a, Sqlx op, Integer b)
        {
            Integer r;
            switch (op)
            {
                case Sqlx.PLUS: r = a + b; break;
                case Sqlx.MINUS: r = a - b; break;
                case Sqlx.TIMES: r = a * b; break;
                case Sqlx.DIVIDE: r = a / b; break;
                default: throw new PEException("PE52");
            }
            if (r.CompareTo(MinLong, 0) >= 0 && r.CompareTo(MaxLong, 0) <= 0)
                return new TInt(tp, (long)r);
            return new TInteger(tp, r);
        }
        /// <summary>
        /// Numeric operations
        /// </summary>
        /// <param name="a">The left Numeric operand</param>
        /// <param name="op">The operator</param>
        /// <param name="b">The right Numeric operand</param>
        /// <returns>The Numeric result</returns>
        static Common.Numeric DecimalOps(Common.Numeric a, Sqlx op, Common.Numeric b)
        {
            switch (op)
            {
                case Sqlx.PLUS:
                    if (a.mantissa == null)
                        return b;
                    if (b.mantissa == null)
                        return a;
                    return a + b;
                case Sqlx.MINUS:
                    if (a.mantissa == null)
                        return -b;
                    if (b.mantissa == null)
                        return a;
                    return a - b;
                case Sqlx.TIMES:
                    if (a.mantissa == null)
                        return a;
                    if (b.mantissa == null)
                        return b;
                    return a * b;
                case Sqlx.DIVIDE:
                    if (a.mantissa == null)
                        return a;
                    if (b.mantissa == null)
                        return b;
                    return Common.Numeric.Divide(a, b, (a.precision > b.precision) ? a.precision : b.precision);
                default: throw new PEException("PE53");
            }
        }
        /// <summary>
        /// double operations
        /// </summary>
        /// <param name="a">The left double operand</param>
        /// <param name="op">The operator</param>
        /// <param name="b">The right double operand</param>
        /// <returns>The double result</returns>
        static double DoubleOps(double? aa, Sqlx op, double? bb)
        {
            if (aa == null || bb == null)
                return double.NaN;
            var a = aa.Value;
            var b = bb.Value;
            switch (op)
            {
                case Sqlx.PLUS: return a + b;
                case Sqlx.MINUS: return a - b;
                case Sqlx.TIMES: return a * b;
                case Sqlx.DIVIDE: return a / b;
                default: throw new PEException("PE54");
            }
        }
        TInterval DateTimeDifference(DateTime a, DateTime b)
        {
            Interval it;
            switch (start)
            {
                case Sqlx.YEAR:
                    if (end == Sqlx.MONTH) goto case Sqlx.MONTH;
                    it = new Interval(a.Year - b.Year, 0);
                    break;
                case Sqlx.MONTH:
                    it = new Interval(0, (a.Year - b.Year) * 12 + a.Month - b.Month);
                    break;
                default:
                    it = new Interval(a.Ticks - b.Ticks); break;
            }
            return new TInterval(it);
        }
        internal byte BsonType()
        {
            switch (Equivalent(kind))
            {
                case Sqlx.Null: return 10;
                case Sqlx.REAL: return 1;
                case Sqlx.CHAR: return 2;
                case Sqlx.DOCUMENT: return 3;
                case Sqlx.DOCARRAY: return 4;
                case Sqlx.BLOB: return 5;
                default: return 6;
                case Sqlx.OBJECT: return 7;
                case Sqlx.BOOLEAN: return 8;
                case Sqlx.TIMESTAMP: return 9;
                case Sqlx.NULL: return 10;
                case Sqlx.ROUTINE: return 13;
                case Sqlx.NUMERIC: return 19; // Decimal subtype added for Pyrrho
                case Sqlx.INTEGER: return 16;
            }
        }
        /// <summary>
        /// Compute the datatype resulting from limiting this by another datatype constraint.
        /// this.LimitBy(union) gives this if this is in the union, otherwise
        /// this.LimitBy(dt) gives the same result as dt.LimitBy(this).
        /// </summary>
        /// <param name="dt"></param>
        /// <returns></returns>
        internal virtual Domain Constrain(Context cx,long lp,Domain dt)
        {
            var ce = dt.elType;
            if (kind == Sqlx.SENSITIVE)
            {
                if (dt.kind == Sqlx.SENSITIVE)
                {
                    var ts = elType.Constrain(cx,lp,ce);
                    if (ts == null)
                        return null;
                    return ts.Equals(elType) ? this : ts.Equals(ce) ? dt :
                        (Domain)cx.Add(new Domain(cx.GetUid(),Sqlx.SENSITIVE, ts));
                }
                var tt = elType.Constrain(cx,lp,dt);
                if (tt == null)
                    return null;
                return tt.Equals(elType) ? this : 
                    (Domain)cx.Add(new Domain(cx.GetUid(),Sqlx.SENSITIVE, tt));
            }
            if (dt.kind == Sqlx.SENSITIVE)
            {
                var tu = Constrain(cx,lp,ce);
                if (tu == null)
                    return null;
                return tu.Equals(dt.elType) ? dt : 
                    (Domain)cx.Add(new Domain(cx.GetUid(),Sqlx.SENSITIVE, tu));
            }
            if (dt == null || dt == Null)
                return this;
            var ki = Equivalent(kind);
            var dk = Equivalent(dt.kind);
            var r = this;
            if (ki == dk)
                return this;
            if ((ki == Sqlx.ARRAY || ki == Sqlx.MULTISET) && ki == dk && ce == null)
                return this;
            if (ki == Sqlx.CONTENT || ki == Sqlx.VALUE)
                return dt;
            if (dk == Sqlx.CONTENT || dk == Sqlx.VALUE || Equals(dt))
                return this;
            if (ki == Sqlx.REAL && dk == Sqlx.NUMERIC)
                return dt;
            if (kind == Sqlx.NUMERIC && dt.kind == Sqlx.INTEGER)
                return null;
            if (kind == Sqlx.REAL && dt.kind == Sqlx.INTEGER)
                return null;
            if (kind == Sqlx.INTERVAL && dt.kind == Sqlx.INTERVAL)
            {
                int s = IntervalPart(start), ds = IntervalPart(dt.start),
                    e = IntervalPart(end), de = IntervalPart(dt.end);
                if (s >= 0 && (s <= 1 != ds <= 1))
                    return null;
                if (s <= ds && (e >= de || de < 0))
                    return this;
            }
            if (kind == Sqlx.PASSWORD && dt.kind == Sqlx.CHAR)
                return this;
            if (ki == dk && (kind == Sqlx.ARRAY || kind == Sqlx.MULTISET))
            {
                if (elType!=Null)
                    return dt;
                var ect = elType.Constrain(cx,lp,ce);
                if (ect == elType)
                    return this;
                return dt;
            }
            if (ki == Sqlx.UNION && dk != Sqlx.UNION)
                for (var b = unionOf.First(); b != null; b = b.Next())
                    if (b.key() is Domain dm && dm.Constrain(cx,lp,dt) != null)
                        return dm;
            if (ki != Sqlx.UNION && dk == Sqlx.UNION)
                for (var b = dt.unionOf.First(); b != null; b = b.Next())
                    if (b.key() is Domain dm && dm.Constrain(cx,lp,this) != null)
                        return this;
            if (ki == Sqlx.UNION && dk == Sqlx.UNION)
            {
                var nt = CTree<Domain,bool>.Empty;
                for (var b = unionOf.First(); b != null; b = b.Next())
                    if (b.key() is Domain db)
                        for (var c = dt.unionOf.First(); c != null; c = c.Next())
                            if (c.key() is Domain dc)
                            {
                                var u = db.Constrain(cx,lp, dc);
                                if (u != null)
                                    nt += (u,true);
                            }
                if (nt.Count == 0)
                    return null;
                if (nt.Count == 1)
                    return nt.First().key();
                return (Domain)cx.Add(new Domain(cx.GetUid(), Sqlx.UNION, nt));
            }
            else if (elType != Domain.Null && ce != null)
                r = (Domain)cx.Add(new Domain(cx.GetUid(),kind, elType.LimitBy(cx,lp, ce)));
            else if (ki == Sqlx.ROW && dt == TableType)
                return this;
            else if ((ki == Sqlx.ROW || ki == Sqlx.TYPE) && (dk == Sqlx.ROW || dk == Sqlx.TABLE))
                return dt;
            else if ((ki != Sqlx.ROW && ki != dk) || (ki == Sqlx.ROW && dk != Sqlx.ROW) ||
                    (elType==Domain.Null) != (ce == null) || orderFunc != dt.orderFunc || orderflags != dt.orderflags)
                return null;
            if ((dt.prec != 0 && prec != 0 && prec != dt.prec) || (dt.scale != 0 && scale != 0 && scale != dt.scale) ||
                (dt.iri != "" && iri != "" && iri != dt.iri) || start != dt.start || end != dt.end ||
                (dt.charSet != CharSet.UCS && charSet != CharSet.UCS && charSet != dt.charSet) ||
                (dt.culture != CultureInfo.InvariantCulture && culture != CultureInfo.InvariantCulture && culture != dt.culture))
                //             (dt.defaultValue != "" && defaultValue != "" && defaultValue != dt.defaultValue)
                return null;
            if ((prec != dt.prec || scale != dt.scale || iri != dt.iri ||
                charSet != dt.charSet || culture != dt.culture ||
                defaultValue != dt.defaultValue) && (r == this || r == dt))
            {
                var m = r.mem;
                if (dt.prec != 0 && dt.prec != r.prec)
                    m += (Precision, dt.prec);
                else if (prec != 0 && prec != r.prec)
                    m += (Precision, r.prec);
                if (dt.scale != 0 && dt.scale != r.scale)
                    m += (Scale, dt.scale);
                else if (scale != 0 && scale != r.scale)
                    m += (Scale, r.scale);
                if (dt.charSet != CharSet.UCS && dt.charSet != r.charSet)
                    m += (Charset, dt.charSet);
                else if (charSet != CharSet.UCS && charSet != r.charSet)
                    m += (Charset, r.charSet);
                if (dt.culture != CultureInfo.InvariantCulture && dt.culture != r.culture)
                    m += (Culture, dt.culture);
                else if (culture != CultureInfo.InvariantCulture && culture != r.culture)
                    m += (Culture, r.culture);
                if (!dt.defaultValue.IsNull && dt.defaultValue != r.defaultValue)
                    m += (Default, dt.defaultValue);
                else if (!defaultValue.IsNull && defaultValue != r.defaultValue)
                    m += (Default, r.defaultValue);
                if (m != r.mem)
                    r = new Domain(cx.GetUid(), r.kind, m);
            }
            return r;
        }
        internal Domain LimitBy(Context cx,long lp,Domain dt)
        {
            return Constrain(cx,lp,dt) ?? this;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Domain(defpos,m);
        }
        internal override void _Add(Context cx)
        {
            if (structure >= 0 && cx.db.objects[structure] is Table t)
                t._Add(cx);
            base._Add(cx);
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (defpos>=0)?(Domain)base._Relocate(cx):this;
            if (constraints.Count>0)
                r += (Constraints, cx.Fix(constraints));
            if (elType !=Null)
                r += (Element, cx.db.types[elType]);
            if (orderFunc!=null)
                r += (OrderFunc, orderFunc.Relocate(cx));
            if (representation.Count>0)
                r += (Representation, cx.Fix(representation));
            if (rowType.Count>0)
                r += (RowType, cx.Fix(rowType));
            if (structure > 0)
                r += (Structure, cx.Fix(structure));
            if (r.mem.Contains(Default) && r.defaultValue!=TNull.Value)
                r += (Default, r.defaultValue.Relocate(cx));
            var db = cx.db;
            var ts = db.types;
            if (ts.Contains(r))
            {
                var p = ts[r];
                if (p!=r.defpos)
                    return (Domain)(cx.obs[p]??cx.db.objects[p]);
                else
                    cx.db += (Database.Types, ts+(r, r.defpos));
            }
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var np = cx.Fix(defpos);
            if (np == defpos && np < Transaction.Executables)
                return this;
            if (cx.done.Contains(np))
                return cx.done[np];
            var r = (Domain)Relocate(np);
            var nc = cx.Fix(constraints);
            if (constraints!=nc)
                r += (Constraints, nc);
            var no = orderFunc?.Fix(cx);
            if (orderFunc != no)
                r += (OrderFunc, no);
            var nr = cx.Fix(representation);
            if (representation !=nr)
                r += (Representation, nr);
            var et = elType.Fix(cx);
            if (et != elType)
                r += (Element, et);
            var st = cx.Fix(structure);
            if (st != structure)
                r += (Structure, st);
            var nt = cx.Fix(rowType);
            if (rowType!=nt)
                r += (RowType, nt);
            var na = cx.Fix(aggs);
            if (aggs != na)
                r += (Aggs, na);
            if (mem.Contains(Default) && defaultValue?.dataType!=null)
                r += (Default, defaultValue.Fix(cx));
            cx.obs += (np, r);
            cx.done += (np, r);
            return r;
        }
        internal override DBObject _Replace(Context cx, DBObject was, DBObject now)
        {
            if (defpos < 0)
                return this;
            if (cx.done.Contains(defpos))
                return cx.done[defpos];
            var r = this;
            var ch = false;
            var cs = CTree<long, bool>.Empty;
            for (var b = r.constraints?.First(); b != null; b = b.Next())
            {
                var ck = (Check)cx._Replace(b.key(), was, now);
                ch = ch || b.key() != ck.defpos;
                cs += (ck.defpos, true);
            }
            if (ch)
                r += (Constraints, cs);
            if (r.elType != Null)
            {
                var e = cx._Dom(r.elType)._Replace(cx, was, now);
                if (e != elType)
                    r += (Element, e);
            }
            var orf = orderFunc?.Replace(cx, was, now);
            if (orf != orderFunc)
                r += (OrderFunc, orf.defpos);
            var rs = CTree<long,Domain>.Empty;
            ch = false;
            for (var b = representation.First(); b != null; b = b.Next())
            {
                var od = b.value();
                var k = b.key();
                var rr = (Domain)od?._Replace(cx, was, now);
                if (k == was.defpos)
                {
                    k = now.defpos;
                    rr = (Domain)(cx.done[now.domain]??cx._Dom(now)); 
                }
                if (rr != od || k != b.key())
                    ch = true;
                rs += (k, rr);
            }
            if (ch)
                r += (Representation, rs);
            var rt = CList<long>.Empty;
            ch = false;
            for (var b=rowType.First();b!=null;b=b.Next())
            {
                var p = b.value();
                var np = p;
                if (p==was.defpos)
                {
                    np = now.defpos;
                    ch = ch || p != np;
                }
                rt += np;
            }
            if (ch)
            {
                r += (RowType, rt);
                cx.Add(r);
            }
            cx.done += (defpos, r);
            return r;
        }
        internal Domain Replaced(Context cx)
        {
            var r = this;
            var ch = false;
            var rs = CTree<long, Domain>.Empty;
            for (var b = representation.First(); b != null; b = b.Next())
            {
                var od = b.value();
                var nk = cx.done[b.key()]?.defpos??b.key();
                var rr = od.Replaced(cx);
                if (rr != od || nk != b.key())
                    ch = true;
                rs += (nk, rr);
            }
            if (ch)
                r += (Representation, rs);
            var rt = CList<long>.Empty;
            ch = false;
            for (var b = rowType.First(); b != null; b = b.Next())
            {
                var p = b.value();
                var np = cx.done[p]?.defpos??p;
                if (p != np)
                    ch = true;
                rt += np;
            }
            if (ch)
                r += (RowType, rt);
            return r;
        }
        public string NameFor(Context cx, long p, int i)
        {
            var sv = cx.obs[p];
            var n = sv?.alias ?? sv?.name;
            return cx.Inf(p)?.name ?? n ?? ("Col"+i);
        }
        internal static TypedValue Now => new TDateTime(Timestamp, DateTime.Now);
        internal static TypedValue MaxDate => new TDateTime(Timestamp, DateTime.MaxValue);
        /// <summary>
        /// Provide an XML version of the type information for the client
        /// </summary>
        /// <param name="transaction"></param>
        /// <returns>an XML string</returns>
        internal string XmlInfo()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("<PyrrhoDBType kind=\"" + kind + "\" ");
            if (iri != null)
                sb.Append(",iri=\"" + iri + "\" ");
            bool empty = true;
            if (kind == Sqlx.ONLY)
                sb.Append(",Only");
            if (elType!=Null)
                sb.Append(",elType=" + elType + "]");
            if (AscDesc != Sqlx.NULL)
                sb.Append("," + AscDesc);
            if (nulls != Sqlx.NULL)
                sb.Append("," + nulls);
            if (prec != 0)
                sb.Append(",P=" + prec);
            if (scale != 0)
                sb.Append(",S=" + scale);
            if (start != Sqlx.NULL)
                sb.Append(",T=" + start);
            if (end != Sqlx.NULL)
                sb.Append(",E=" + end);
            if (charSet != CharSet.UCS)
                sb.Append("," + charSet);
            if (culture != null)
                sb.Append("," + culture.Name);
            if (!defaultValue.IsNull)
                sb.Append(",D=" + defaultValue);
            if (abbrev != null)
                sb.Append(",A=" + abbrev);
            if (empty)
                sb.Append("/>");
            return sb.ToString();
        }
        /// <summary>
        /// Create an XML string for a given value
        /// </summary>
        /// <param name="tr">The transaction: Common doesn't know about this</param>
        /// <param name="ob">the value to represent</param>
        /// <returns>the corresponding XML</returns>
        public string Xml(Context cx,long defpos, TypedValue ob)
        {
            if (ob == null)
                return "";
            StringBuilder sb = new StringBuilder();
            switch (kind)
            {
                default:
                    //          sb.Append("type=\"" + ob.DataType.ToString() + "\">");
                    sb.Append(ob.ToString());
                    break;
                case Sqlx.ARRAY:
                    {
                        var a = (TArray)ob;
                        //           sb.Append("type=\"array\">");
                        var ep = cx.db.types[elType];
                        for (int j = 0; j < a.Length; j++)
                            sb.Append("<item " + elType.Xml(cx, ep, a[j]) + "</item>");
                        break;
                    }
                case Sqlx.MULTISET:
                    {
                        var m = (TMultiset)ob;
                        //          sb.Append("type=\"multiset\">");
                        var ep = cx.db.types[elType];
                        for (var e = m.tree.First(); e != null; e = e.Next())
                            sb.Append("<item " + elType.Xml(cx, ep, e.key()) + "</item>");
                        break;
                    }
                case Sqlx.ROW:
                case Sqlx.TABLE:
                case Sqlx.TYPE:
                    {
                        TRow r = (TRow)ob;
                        if (r.Length == 0)
                            throw new DBException("2200N").ISO();
                        var ro = cx.db.role;
                        var sc = sb;
                        if (ro != null)
                            sb.Append("<" + ro.name);
                        var ss = new string[r.Length];
                        var empty = true;
                        var i = 0;
                        for (var b=r.dataType.representation.First();b!=null;b=b.Next(), i++)
                        {
                            var tv = r[i];
                            if (tv == null)
                                continue;
                            var kn = b.key();
                            var p = tv.dataType;
                            var m = (cx.db.role.infos[kn] as ObInfo).metadata;
                            if (tv != null && !tv.IsNull && m != null && m.Contains(Sqlx.ATTRIBUTE))
                                sb.Append(" " + kn + "=\"" + tv.ToString() + "\"");
                            else if (tv != null && !tv.IsNull)
                            {
                                ss[i] = "<" + kn + " type=\"" + p.ToString() + "\">" +
                                    p.Xml(cx, defpos, tv) + "</" + kn + ">";
                                empty = false;
                            }
                        }
                        if (ro != null)
                        {
                            if (empty)
                                sb.Append("/");
                            sb.Append(">");
                        }
                        for (int j = 0; j < ss.Length; j++)
                            if (ss[j] != null)
                                sb.Append(ss[j]);
                        if (ro != null && !empty)
                            sb.Append("</" + ro.name + ">");
                        break;
                    }
                case Sqlx.PASSWORD: sb.Append("*********"); break;
            }
            return sb.ToString();
        }
        internal override DBObject Relocate(long dp)
        {
            return new Domain(dp,mem);
        }
    }
    // shareable as of 26 April 2021
    internal class StandardDataType : Domain
    {
        public static BTree<Sqlx, Domain> types = BTree<Sqlx, Domain>.Empty;
        internal StandardDataType(Sqlx t, Common.OrderCategory oc = Common.OrderCategory.None, 
            Domain o = null, string c = null, BTree<long, object> u = null)
            : base(--_uid, t, _Mem(oc, o, c, u))
        {
            types += (t, this);
            Context._system.db = Database._system;
        }
        internal override DBObject New(Context cx, BTree<long, object> m)
        {
            return base.New(cx, m);
        }
        static BTree<long, object> _Mem(Common.OrderCategory oc, Domain o, string c, BTree<long, object> u)
        {
            if (u == null || o==null)
                u = BTree<long, object>.Empty;
            if (o != null)
                u += (Element, o.defpos);
            if (!u.Contains(Descending))
                u += (Descending, Sqlx.ASC);
            if (!u.Contains(OrderCategory))
                u += (OrderCategory, oc);
            return u;
        }
     /*   public override int CompareTo(object obj)
        {
            var that = (Domain)obj;
            if (that == null)
                return 1;
            return defpos.CompareTo(that.defpos);
        } */
        public override string ToString()
        {
            return kind.ToString();
        }
    }
    /// <summary>
    /// Security labels.
    /// Access rules: clearance C can access classification z if C.maxlevel>=z and 
    ///  can update if classification matches C.minlevel.
    /// Clearance allows minlevel LEQ laxlevel.
    /// For classification minlevel==maxlevel always.
    /// In addition clearance must have all the references of the classification 
    /// and at least one of the groups.
    /// The database uses a cache of level descriptors called levels.
    /// shareable
    /// </summary>
    public class Level : IComparable
    {
        public readonly byte minLevel = 0, maxLevel = 0; // D=0, C=1, B=3, A=3
        public readonly BTree<string, bool> groups = BTree<string, bool>.Empty;
        public readonly BTree<string, bool> references = BTree<string, bool>.Empty;
        public static Level D = new Level();
        Level() { }
        public Level(byte min, byte max, BTree<string, bool> g, BTree<string, bool> r)
        {
            minLevel = min; maxLevel = max; groups = g; references = r;
        }
        internal static void SerialiseLevel(Writer wr, Level lev)
        {
            if (wr.cx.db.levels.Contains(lev))
                wr.PutLong(wr.cx.db.levels[lev]);
            else
            {
                wr.PutLong(-1L);
                wr.cx.db += (lev,wr.Length);
                wr.WriteByte(lev.minLevel);
                wr.WriteByte(lev.maxLevel);
                wr.PutInt((int)lev.groups.Count);
                for (var b = lev.groups.First(); b != null; b = b.Next())
                    wr.PutString(b.key());
                wr.PutInt((int)lev.references.Count);
                for (var b = lev.references.First(); b != null; b = b.Next())
                    wr.PutString(b.key());
            }
        }
        internal static Level DeserialiseLevel(Reader rd)
        {
            Level lev;
            var lp = rd.GetLong();
            if (lp != -1)
            {
                if (rd is Reader rdr)
                    return rdr.context.db.cache[lp];
                else
                    return DeserialiseLevel(new Reader(rd.context.db, lp));
            }
            else
            {
                lp = rd.Position;
                var min = (byte)rd.ReadByte();
                var max = (byte)rd.ReadByte();
                var gps = BTree<string, bool>.Empty;
                var n = rd.GetInt();
                for (var i = 0; i < n; i++)
                    gps += (rd.GetString(), true);
                var rfs = BTree<string, bool>.Empty;
                n = rd.GetInt();
                for (var i = 0; i < n; i++)
                    rfs += (rd.GetString(), true);
                lev = new Level(min, max, gps, rfs);
                if (rd is Reader rr)
                    rr.context.db += (lev, lp);
            }
            return lev;
        }
        public bool ClearanceAllows(Level classification)
        {
            if (maxLevel < classification.minLevel)
                return false;
            for (var b = classification.references.First(); b != null; b = b.Next())
                if (!references.Contains(b.key()))
                    return false;
            if (classification.groups.Count == 0)
                return true;
            for (var b = groups.First(); b != null; b = b.Next())
                if (classification.groups.Contains(b.key()))
                    return true;
            return false;
        }
        public Level ForInsert(Level classification)
        {
            if (minLevel == 0)
                return this;
            var gps = BTree<string, bool>.Empty;
            for (var b = groups.First(); b != null; b = b.Next())
                if (classification.groups.Contains(b.key()))
                    gps +=(b.key(), true);
            return new Level(minLevel, minLevel, gps, classification.references);
        }
        public override bool Equals(object obj)
        {
            var that = obj as Level;
            if (that == null || minLevel != that.minLevel || maxLevel != that.maxLevel
                || groups.Count != that.groups.Count || references.Count != that.references.Count)
                return false;
            for (var b = references.First(); b != null; b = b.Next())
                if (!that.references.Contains(b.key()))
                    return false;
            for (var b = groups.First(); b != null; b = b.Next())
                if (!that.groups.Contains(b.key()))
                    return false;
            return true;
        }
        public override int GetHashCode()
        {
            return (int)(minLevel + maxLevel + groups.Count + references.Count);
        }
        char For(byte b)
        {
            return (char)('D' - b);
        }
        void Append(StringBuilder sb, BTree<string, bool> t, char s, char e)
        {
            var cm = "";
            sb.Append(s);
            for (var b = t.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ","; sb.Append(b.key());
            }
            sb.Append(e);
        }
        public void Append(StringBuilder sb)
        {
            if (maxLevel == 0 && groups.Count == 0 && references.Count == 0)
                return;
            sb.Append(' ');
            sb.Append(For(minLevel));
            if (maxLevel != minLevel)
            {
                sb.Append('-'); sb.Append(For(maxLevel));
            }
            if (groups.Count != 0)
                Append(sb, groups, '{', '}');
            if (references.Count != 0)
                Append(sb, references, '[', ']');
        }
        public override string ToString()
        {
            var sb = new StringBuilder();
            Append(sb);
            return sb.ToString();
        }
        public static Level Parse(string s)
        {
            var sc = new Scanner(-1, s.ToCharArray(), 0);
            byte low=0, high;
            var gps = BTree<string, bool>.Empty;
            var rfs = BTree<string, bool>.Empty;
            var ch = sc.Advance();
            if (ch >= 'A' && ch <= 'D')
                low = (byte)('D' - ch);
            ch = sc.Advance();
            if (sc.ch == '-')
            {
                ch = sc.Advance();
                high = (byte)('D' - ch);
            } else
                high = low;
            ch = sc.Advance();
            if (ch=='{')
            {
                sc.Advance();
                do
                {
                    var g = sc.GetName();
                    gps += (g, true);
                } while (sc.ch == ',');
            }
            if (ch == '[')
            {
                sc.Advance();
                do
                {
                    var g = sc.GetName();
                    rfs += (g, true);
                } while (sc.ch == ',');
            }
            return new Level(low, high, gps, rfs);
        }
        public int CompareTo(object obj)
        {
            Level that = (Level)obj;
            int c = minLevel.CompareTo(that.minLevel);
            if (c != 0)
                return c;
            c = maxLevel.CompareTo(that.maxLevel);
            if (c != 0)
                return c;
            var b = groups.First();
            var tb = that.groups.First();
            for (;c==0 && b!=null && tb!=null;b=b.Next(),tb=tb.Next())
                c = b.key().CompareTo(tb.key());
            if (c != 0)
                return c;
            if (b != null)
                return 1;
            if (tb != null)
                return -1;
            b = references.First();
            tb = that.references.First();
            for (; c == 0 && b != null && tb != null; b = b.Next(), tb = tb.Next())
                c = b.key().CompareTo(tb.key());
            if (b != null)
                return 1;
            if (tb != null)
                return -1;
            return c;
        }

    }
    // shareable as of 26 April 2021
    internal class Period
    {
        public readonly TypedValue start, end;
        public Period(TypedValue s, TypedValue e)
        {
            start = s; end = e;
        }
        public Period(Period p) : this(p.start, p.end) { }
        public override string ToString()
        {
            return "period(" + start.ToString() + "," + end.ToString() + ")";
        }
    }
    /// <summary>
    /// A class for the lowset levels of Parsing: white space etc
    /// </summary>
    internal class Scanner
    {
        /// <summary>
        /// the given input as an array of chars
        /// </summary>
        internal char[] input;
        /// <summary>
        /// The current position in the input
        /// </summary>
        internal int pos;
        /// <summary>
        /// the length of the input
        /// </summary>
        internal int len;
        /// <summary>
        /// The transaction uid
        /// </summary>
        internal long tid;
        /// <summary>
        /// the current character
        /// </summary>
        internal char ch;
        /// <summary>
        /// Whether to use XML conventions
        /// </summary>
        internal string mime = "text/plain";
        internal Context _cx = null;
        /// <summary>
        /// Constructor: prepare the scanner
        /// Invariant: ch==input[pos]
        /// </summary>
        /// <param name="s">the input array</param>
        /// <param name="p">the starting position</param>
        internal Scanner(long t,char[] s, int p)
        {
            tid = t;
            input = s;
            len = input.Length;
            pos = p;
            ch = (p < len) ? input[p] : '\0';
        }
        /// <summary>
        /// Constructor: prepare the scanner
        /// Invariant: ch==input[pos]
        /// </summary>
        /// <param name="s">the input array</param>
        /// <param name="p">the starting position</param>
        internal Scanner(long t,char[] s, int p, string m,Context cx)
        {
            tid = t;
            input = s;
            mime = m;
            len = input.Length;
            pos = p;
            _cx = cx;
            ch = (p < len) ? input[p] : '\0';
        }
        internal long Position => tid + pos;
        /// <summary>
        /// Consume one character
        /// </summary>
        /// <returns>The character (or 0)</returns>
        internal char Advance()
        {
            pos++;
            if (pos >= len)
                ch = (char)0;
            else
                ch = input[pos];
            return ch;
        }
        /// <summary>
        /// Peek at the next character to be consumed
        /// </summary>
        /// <returns>The character (or 0)</returns>
        internal char Peek()
        {
            if (pos + 1 >= len)
                return (char)0;
            return input[pos + 1];
        }
        /// <summary>
        /// Consume white space
        /// </summary>
        /// <returns>The next non-white space character</returns>
        internal char White()
        {
            while (char.IsWhiteSpace(ch))
                Advance();
            return ch;
        }
        /// <summary>
        /// Consume nonwhite space
        /// </summary>
        /// <returns></returns>
        internal string NonWhite()
        {
            int st = pos;
            while (!char.IsWhiteSpace(ch))
                Advance();
            return new string(input, st, pos - st);
        }
        /// <summary>
        /// See if the input matches the given string,
        /// and advance past it if so
        /// </summary>
        /// <param name="mat">The string to test</param>
        /// <returns>Whether we matched and advanced</returns>
        internal bool Match(string mat)
        {
            int n = mat.Length;
            if (n + pos > len)
                return false;
            for (int j = 0; j < n; j++)
                if (input[pos + j] != mat[j])
                    return false;
            pos += n - 1;
            Advance();
            return true;
        }
        /// <summary>
        /// See if the input matches the given string ignoring differences in case,
        /// and advance past it if so
        /// </summary>
        /// <param name="mat">The string to test (guaranteed upper case)</param>
        /// <returns>whether we matched and advanced</returns>
        internal bool MatchNC(string mat)
        {
            int n = mat.Length;
            if (n + pos > len)
                return false;
            for (int j = 0; j < n; j++)
                if (char.ToUpper(input[pos + j]) != mat[j])
                    return false;
            pos += n - 1;
            Advance();
            return true;
        }
        /// <summary>
        /// Construct a string out of a portion of the input.
        /// </summary>
        /// <param name="st">The start</param>
        /// <param name="len">The length</param>
        /// <returns>the string</returns>
        internal string String(int st, int len)
        {
            return new string(input, st, len);
        }
        /// <summary>
        /// This string comparison routine works for Unicode strings
        /// including non-normalized strings.
        /// We compare the strings codepoint by codepoint.
        /// string.CompareTo silently normalizes strings first so that
        /// strings with different codpoints or even lengths can appear to be
        /// equal.
        /// </summary>
        /// <param name="s">a string</param>
        /// <param name="t">another string</param>
        /// <returns>neg,0,pos according as s lt, eq or gt t</returns>
        internal static int Compare(string s, string t)
        {
            int n = s.Length;
            int m = t.Length;
            for (int j = 0; j < n && j < m; j++)
            {
                char c = s[j];
                char d = t[j];
                if (c != d)
                    return c - d;
            }
            return n - m;
        }
        /// <summary>
        /// Watch for named value (REST service)
        /// </summary>
        /// <returns>a name or null</returns>
        internal string GetName()
        {
            int st = pos;
            if (ch == '"')
            {
                Advance();
                st = pos;
                while (ch != '"')
                    Advance();
                Advance();
                return new string(input, st, pos - st - 1);
            }
            if (Char.IsLetter(ch))
            {
                while (Char.IsLetterOrDigit(ch))
                    Advance();
            }
            return new string(input, st, pos - st + 1);
        }
    }
    /// <summary>
    /// A class for RdfLiterals
    /// // shareable
    /// </summary>
    internal class RdfLiteral : TChar
    {
        public readonly object val; // the binary version
        public readonly bool name; // whether str matches val
        public RdfLiteral(Domain t, string s, object v, bool c) : base(t, s)
        {
            val = v;
            name = c;
        }
        internal static RdfLiteral New(Domain it, string v)
        {
            if (it.iri == IriRef.STRING || v == "")
                return new RdfLiteral(it, v, "", false); // non-name to supply datatype for strings
            return new RdfLiteral(it, v, v, false);
        }
        public override string ToString()
        {
            return base.ToString();
        }
    }
    internal class IriRef
    {
        private IriRef() { }
        public readonly static string xsd = "http://www.w3.org/2001/XMLSchema#";
        public readonly static string BOOL = xsd + "boolean";
        public readonly static string INTEGER = xsd + "integer";
        public readonly static string INT = xsd + "int";
        public readonly static string LONG = xsd + "long";
        public readonly static string SHORT = xsd + "short";
        public readonly static string BYTE = xsd + "byte";
        public readonly static string UNSIGNEDINT = xsd + "unsignedInt";
        public readonly static string UNSIGNEDLONG = xsd + "unsignedLong";
        public readonly static string UNSIGNEDSHORT = xsd + "unsignedShort";
        public readonly static string UNSIGNEDBYTE = xsd + "unsignedByte";
        public readonly static string NONPOSITIVEINTEGER = xsd + "nonPositiveInteger";
        public readonly static string NEGATIVEINTEGER = xsd + "negativeInteger";
        public readonly static string NONNEGATIVEINTEGER = xsd + "nonNegativeInteger";
        public readonly static string POSITIVEINTEGER = xsd + "positiveInteger";
        public readonly static string DECIMAL = xsd + "decimal";
        public readonly static string FLOAT = xsd + "float";
        public readonly static string DOUBLE = xsd + "double";
        public readonly static string STRING = xsd + "string";
        public readonly static string DATETIME = xsd + "dateTime";
        public readonly static string DATE = xsd + "date";
    }
    // shareable as of 26 April 2021
    internal class UDType: Domain
    {
        internal const long
            Under = -90; // Domain
        public UDType super => (UDType)mem[Under];
        public override CList<long> rowType
        {
            get
            {
                var r = CList<long>.Empty;
                for (var b = representation.First(); b != null; b = b.Next())
                    r += b.key();
                return r;
            }
        }
        public CTree<long,string> methods =>
            (CTree<long,string>)mem[Database.Procedures] ?? CTree<long,string>.Empty;
        public UDType(PType pt,Context cx) : base(pt.ppos,_Mem(pt,cx)) 
        { }
        internal UDType(long dp, Sqlx k, BTree<long, object> m) : base(dp, k, m) 
        { }
        protected UDType(long dp,BTree<long,object>m) :base(dp,m)
        { }
        public static UDType operator+(UDType t,(long,object)x)
        {
            return (UDType)t.New(t.mem + x);
        }
        public static UDType operator -(UDType t, long x)
        {
            return (UDType)t.New(t.mem - x);
        }
        public static BTree<long,object> _Mem(PType pt,Context cx)
        {
            var r = (pt.structure?.mem??BTree<long,object>.Empty) 
                + (Kind, Sqlx.TYPE) + (Name,pt.name);
            var dvs = CTree<long, TypedValue>.Empty;
            var rt = (pt.under?.rowType ?? CList<long>.Empty) +  
                (pt.structure?.rowType ?? CList<long>.Empty);
            var rs = (pt.under?.representation??CTree<long,Domain>.Empty)
                + (pt.structure?.representation??CTree<long,Domain>.Empty);
            for (var b = rs.First(); b != null; b = b.Next())
            {
                var c = b.key();
                dvs += (c, b.value().defaultValue);
            }
            r = r + (RowType, rt) + (Representation, rs) + (_Framing, pt.framing);
            r += (DefaultRowValues, dvs);
            if (pt.under != null)
                r += (Under,pt.under.defpos);
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new UDType(defpos,m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new UDType(dp,mem);
        }
        internal void Defs(Context cx)
        {
            var lp = cx.GetUid();
            for (var b = representation.First(); b != null; b = b.Next())
            {
                var dm = b.value();
                dm.Instance(lp,cx, null);
                var p = b.key();
                var c = (TableColumn)cx.db.objects[p];
                var oi = (ObInfo)cx.db.role.infos[p];
                cx.Add(c + (_Domain, oi.domain));
                var px = cx.Ix(p);
                var ic = new Ident(oi.name, px);
                cx.defs += (ic, px);
                cx.iim += (ic.iix.dp, px);
            }
            for (var b = methods.First();b!=null;b=b.Next())
            {
                var mp = b.key();
                var me = (Method)cx.db.objects[mp];
                me.Instance(lp, cx, null);
            }
        }
        internal override DBObject Instance(long lp,Context cx, Domain q, BList<Ident> cs = null)
        {
            if (cx.obs.Contains(defpos))
                return this;
            cx.Add(this);
            if (cx.db.objects[structure] is Table st && 
                cx.db.role.infos[structure] is ObInfo si)
                cx.Add(st + (_Domain, si.domain));
            Defs(cx);
            return base.Instance(lp,cx, q);
        }
        public override bool EqualOrStrongSubtypeOf(Domain dt)
        {
            var ki = Equivalent(kind);
            var dk = Equivalent(dt.kind);
            if (ki == Sqlx.ONLY)
                return super.Equals(dt);
            for (var s = this; s != null; s = s.super)
                if (s.Equals(dt))
                    return true;
            return base.EqualOrStrongSubtypeOf(dt);
        }
        public override bool CanTakeValueOf(Domain dt)
        {
            if (dt?.kind == Sqlx.ONLY)
                dt = ((UDType)dt).super;
            return base.CanTakeValueOf(dt);
        }
        public override bool HasValue(Context cx, TypedValue v)
        {
            var ki = Equivalent(kind);
            if (ki == Sqlx.UNION)
            {
                for (var d = v.dataType; d != null; d = (d as UDType)?.super)
                    if (mem.Contains(cx.db.types[d]))
                        return true;
                return false;
            }
            return base.HasValue(cx, v);
        }
        internal override Domain Constrain(Context cx, long lp, Domain dt)
        {
            var ki = Equivalent(kind);
            var dk = Equivalent(dt.kind);
            if (dk == Sqlx.ONLY && Equals(((UDType)dt).super))
                return dt;
            if (ki == Sqlx.ONLY && super.Equals(dt))
                return this;
            return base.Constrain(cx, lp, dt);
        }
        internal override DBObject _Replace(Context cx, DBObject was, DBObject now)
        {
            var r = (UDType)base._Replace(cx, was, now);
            if (r.super?._Replace(cx, was, now) is Domain und
    && und != super)
                r += (Under, und);
            if (r!=this)
                r= (UDType)New(cx,r.mem);
            cx.done += (defpos, r);
            return r;
        }
        internal override Basis _Fix(Context cx)
        {
            var r = (UDType)base._Fix(cx);
            var ns = super?.Fix(cx);
            if (super != ns)
                r += (Under, ns);
            if (defaultString == "")
                r -= Default;
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (UDType)base._Relocate(cx);
            if (super != null)
                r += (Under, super.Relocate(cx));
            if (defaultString == "")
                r -= Default;
            return r;
        }
        public override void PutDataType(Domain nt, Writer wr)
        {
            if (nt.kind == Sqlx.ONLY)
            {
                var at = (nt as UDType).super;
                nt.PutDataType(nt, wr); //??
                return;
            }
            base.PutDataType(nt, wr);
        }
        /// <summary>
        /// Generate a row for the Role$Class table: includes a C# class definition
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="from"></param>
        /// <param name="_enu"></param>
        /// <returns></returns>
        internal override TRow RoleClassValue(Context cx, DBObject from, ABookmark<long, object> _enu)
        {
            var ro = cx.db.role;
            var md = (ObInfo)ro.infos[defpos];
            var sb = new StringBuilder("using System;\r\nusing Pyrrho;\r\n");
            sb.Append("\r\n/// <summary>\r\n");
            sb.Append("/// Class " + md.name + " from Database " + cx.db.name
                + ", Role " + ro.name + "\r\n");
            if (md.description != "")
                sb.Append("/// " + md.description + "\r\n");
            sb.Append("/// </summary>\r\n");
            sb.Append("public class " + md.name + ((super!=null)?" : "+super.name:"") + " {\r\n");
            for (var b = md.dataType.representation.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var dt = b.value();
                var tn = (dt.kind == Sqlx.TYPE) ? dt.name : dt.SystemType.Name;
                FieldType(cx, sb, dt);
                var ci = (ObInfo)cx.db.role.infos[p];
                if (ci.description?.Length > 1)
                    sb.Append("  // " + ci.description + "\r\n");
                sb.Append("  public " + tn + " " + ci.name + ";");
                sb.Append("\r\n");
            }
            sb.Append("}\r\n");
            return new TRow(cx, cx._Dom(from), new TChar(md.name), new TChar(""),
                new TChar(sb.ToString()));
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(Under)) { sb.Append(" Under="); sb.Append(super); }
            var cm = " Methods: ";
            for (var b=methods.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(Uid(b.key()));
                sb.Append(" ");
                sb.Append(b.value());
            }
            return sb.ToString();
        }
    }
}
