using System.Globalization;
using System.Text;
using System.Xml;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using Pyrrho.Level5;
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
    ///      (depends on TypedValue remaining shareable)
    /// </summary>
    internal class Domain : DBObject, IComparable
    {
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
            Charset = -71, // Pyrrho.CharSet (C E)
            Constraints = -72,  // CTree<long,bool> DBObjects 
            Culture = -73, // System.Globalization.CultureInfo (C E)
            Default = -74, // TypedValue (D C)
            DefaultString = -75, // string
            DefaultRowValues = -216, // CTree<long,TypedValue> SqlValue
            Descending = -76, // Sqlx
            Display = -177, // int
            Element = -77, // Domain
            End = -78, // Sqlx (interval part) (D)
            Kind = -80, // Sqlx
            NotNull = -81, // bool
            NullsFirst = -82, // bool (C)
            _OrderCategory = -83, // OrderCategory
            OrderFunc = -84, // Procedure?  Unlike any other property!
            Precision = -85, // int (D)
            Representation = -87, // CTree<long,Domain> DBObject (gets extended by Matching)
            RowType = -187,  // BList<long?> 
            Scale = -88, // int (D)
            Start = -89, // Sqlx (D)
            Subtypes = -155, // CTree<long,bool> Domain
            SuperShape = -318, // bool
            Under = -90, // Domain
            UnionOf = -91; // CTree<Domain,bool>
        internal static Domain Null, Value, Content, // Pyrrho 5.1 default type for Document entries, from 6.2 for generic scalar value
    Bool, Blob, Char, Password, XML, Int, _Numeric, Real, Date, Timespan, Timestamp,
    Interval, _Level, MTree, // pseudo type for MTree implementations
    Partial, // pseudo type for MTree implementation
    Array, SetType, Multiset, Collection, EdgeEnds, Cursor, UnionNumeric, UnionDate,
    UnionDateNumeric, Exception, Period,
    Document, DocArray, ObjectId, JavaScript, ArgList, // Pyrrho 5.1
    TableType, Row, Delta, Position,
    Metadata, HttpDate, Star, // Pyrrho v7
    _Rvv, Graph; // Rvv is V7 validator type
        internal static UDType TypeSpec;
        internal static NodeType NodeType;
        internal static EdgeType EdgeType;
        static Domain()
        {
            Null = new StandardDataType(Sqlx.Null);
            Value = new StandardDataType(Sqlx.VALUE); // Not known whether scalar or row type
            Content = new StandardDataType(Sqlx.CONTENT); // Pyrrho 5.1 default type for Document entries, from 6.2 for generic scalar value
            Bool = new StandardDataType(Sqlx.BOOLEAN);
            Blob = new StandardDataType(Sqlx.BLOB);
            Char = new StandardDataType(Sqlx.CHAR, OrderCategory.Primitive);
            Password = new StandardDataType(Sqlx.PASSWORD, OrderCategory.Primitive);
            XML = new StandardDataType(Sqlx.XML);
            Int = new StandardDataType(Sqlx.INTEGER, OrderCategory.Primitive);
            _Numeric = new StandardDataType(Sqlx.NUMERIC, OrderCategory.Primitive);
            Real = new StandardDataType(Sqlx.REAL, OrderCategory.Primitive);
            Date = new StandardDataType(Sqlx.DATE, OrderCategory.Primitive);
            HttpDate = new StandardDataType(Sqlx.HTTPDATE, OrderCategory.Primitive);
            Timespan = new StandardDataType(Sqlx.TIME, OrderCategory.Primitive);
            Timestamp = new StandardDataType(Sqlx.TIMESTAMP, OrderCategory.Primitive);
            Interval = new StandardDataType(Sqlx.INTERVAL, OrderCategory.Primitive);
            TypeSpec = new UDType(Sqlx.TYPE);
            _Level = new StandardDataType(Sqlx.LEVEL);
            MTree = new StandardDataType(Sqlx.M); // pseudo type for MTree implementation
            Partial = new StandardDataType(Sqlx.T); // pseudo type for MTree implementation
            Array = new StandardDataType(Sqlx.ARRAY, OrderCategory.None, Content);
            SetType = new StandardDataType(Sqlx.SET, OrderCategory.None, Content);
            EdgeEnds = new StandardDataType(Sqlx.SET, OrderCategory.Primitive, Char);
            Multiset = new StandardDataType(Sqlx.MULTISET, OrderCategory.None, Content);
            Collection = UnionType(--_uid, Array, Multiset);
            Cursor = new StandardDataType(Sqlx.CURSOR);
            UnionNumeric = UnionType(--_uid, Int, _Numeric, Real);
            UnionDate = UnionType(--_uid, Date, Timespan, Timestamp, Interval);
            UnionDateNumeric = UnionType(--_uid, Date, Timespan, Timestamp, Interval, Int, _Numeric, Real);
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
            _Rvv = new StandardDataType(Sqlx.CHECK);
            Position = new StandardDataType(Sqlx.POSITION);
            Metadata = new StandardDataType(Sqlx.METADATA);
            Star = new(--_uid, Sqlx.TIMES, BTree<long, object>.Empty);
            Graph = new StandardDataType(Sqlx.GRAPH);
            NodeType = new NodeType(Sqlx.NODETYPE);
            EdgeType = new EdgeType(Sqlx.EDGETYPE);
        }
        public override Domain domain => this;
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
        public Domain? elType => (Domain?)mem[Element];
        public TypedValue defaultValue => (TypedValue?)mem[Default] ??
            ((defaultRowValues!=CTree<long,TypedValue>.Empty)? new TRow(this,defaultRowValues) 
                        : TNull.Value);
        public string defaultString => (string?)mem[DefaultString] ?? "";
        public CTree<long, TypedValue> defaultRowValues =>
            (CTree<long, TypedValue>?)mem[DefaultRowValues] ?? CTree<long, TypedValue>.Empty;
        public int display => (int)(mem[Display] ?? 0);
        public Domain? super => (Domain?)mem[Under];
        public bool superShape => (bool)(mem[SuperShape] ?? false);
        public CTree<long, bool> subtypes =>
            (CTree<long, bool>)(mem[Subtypes] ?? CTree<long, bool>.Empty);
        public string abbrev => (string?)mem[Abbreviation]??"";
        public CTree<long, bool> constraints => (CTree<long, bool>?)mem[Constraints]??CTree<long,bool>.Empty;
        public CTree<long,Domain> representation => 
            (CTree<long,Domain>?)mem[Representation] ?? CTree<long,Domain>.Empty;
        public virtual BList<long?> rowType => (BList<long?>?)mem[RowType] ?? BList<long?>.Empty;
        public int Length => rowType.Length;
        public Procedure? orderFunc => (Procedure?)mem[OrderFunc];
        public CTree<long,bool> aggs => 
            (CTree<long,bool>?)mem[Aggs]??CTree<long,bool>.Empty;
        public OrderCategory orderflags => (OrderCategory)(mem[_OrderCategory]??OrderCategory.None);
        public CTree<Domain,bool> unionOf => 
            (CTree<Domain,bool>?)mem[UnionOf] ?? CTree<Domain,bool>.Empty;
        internal Domain(Context cx,CTree<long,Domain>rs,BList<long?> rt,BTree<long,ObInfo> ii)
            : this(-1L,_Mem(cx,Sqlx.TABLE,rs,rt,rt.Length)+(Infos,ii)) { }
        internal Domain(long dp,Context cx,Sqlx t, CTree<long, Domain> rs, BList<long?> rt, int ds=0)
            : this(dp,_Mem(cx,t,rs,rt,ds))
        {
            cx.Add(this);
        }
        public Domain(Sqlx t, Context cx, BList<DBObject> vs, int ds=0)
            : this(cx.GetUid(),_Mem(cx,t,vs,ds)) 
        {
            cx.Add(this);
        }
        public Domain(long dp,Context cx,Sqlx t, BList<DBObject> vs, int ds = 0)
    : this(dp, _Mem(cx,t,vs, ds))
        {
            cx.Add(this);
        }
        // A union of standard types
        public Domain(long dp, Sqlx t, CTree<Domain,bool> u)
            : this(dp,BTree<long, object>.Empty + (Kind,t) + (UnionOf,u))
        {
            Database._system += this;
        }
        // A simple standard type
        public Domain(long dp, Sqlx t, BTree<long,object> u)
        : base(dp,u+(Kind,t))
        {
            Database._system += this;
        }
        internal Domain(long dp, BTree<long, object> m) : base(dp, m)
        {  }
        /// <summary>
        /// Allow construction of ad-hoc derived types such as ARRAY, MULTISET, SET, COLLECT
        /// </summary>
        /// <param name="t"></param>
        /// <param name="d"></param>
        public Domain(long dp,Sqlx t, Domain et)
            : base(dp,new BTree<long, object>(Element, et)+(Kind,t))
        {
            if (kind == Sqlx.TYPE && this is not UDType)
                throw new PEException("PE8881");
        }
        protected Domain(long pp, long dp, BTree<long, object>? m = null)
            : base(pp, dp, m)
        {  }
        static BTree<long, object> _Mem(Context cx, Sqlx t, BList<DBObject> vs, int ds = 0)
        {
            var rs = CTree<long, Domain>.Empty;
            var cs = BList<long?>.Empty;
            var ag = CTree<long, bool>.Empty;
            for (var b = vs.First(); b != null; b = b.Next())
            {
                var v = b.value();
                rs += (v.defpos, v.domain);
                cs += v.defpos;
                if (v is SqlValue sv)
                    ag += sv.IsAggregation(cx,CTree<long,bool>.Empty);
            }
            var m = BTree<long,object>.Empty + (Representation, rs) + (RowType, cs)
                + (Aggs, ag) + (Kind, t)
                + (Default, For(t).defaultValue);
            if (ds != 0)
                m += (Display, ds);
            return cx.DoDepth(m);
        }
        static BTree<long, object> _Mem(Context cx,Sqlx t,CTree<long,Domain> rs, BList<long?> cs, int ds = 0)
        {
     //       for (var b = cs.First(); b != null; b = b.Next())
     //           if (b.value() is long p && !rs.Contains(p))
     //               throw new PEException("PE283");
            var d = cx._DepthTVD(rs,1);
            var m =  BTree<long,object>.Empty +(_Depth,d)+ (Representation, rs) + (RowType, cs) + (Kind, t)
                + (Default, For(t).defaultValue);
            if (ds != 0)
                m += (Display, ds);
            return m;
        }
        internal override string NameFor(Context cx)
        {
            if (infos == BTree<long, ObInfo>.Empty) // happens with standard type e.g. INTEGER Prec=11
                return "";
            return base.NameFor(cx);
        }
        internal Domain Scalar(Context cx)
        {
            if ((kind == Sqlx.TABLE || kind == Sqlx.ROW) && Length>0)
                return (Domain)(cx.obs[rowType[0]??-1L]?.domain??Content);
            return this;
        }
        internal Domain Best(Domain dt)
        {
            if (EqualOrStrongSubtypeOf(dt))
                return this;
            if (kind == Sqlx.UNION)
                for (var b = unionOf.First(); b != null; b = b.Next())
                    if (b.key() is not null && EqualOrStrongSubtypeOf(b.key()))
                        return this;
            return dt;
        }
        internal override DBObject Instance(long lp,Context cx, BList<Ident>?cs=null)
        {
            var r = base.Instance(lp,cx);
            for (var b = constraints.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is DBObject ob)
                    cx.Add(ob.Instance(lp,cx));
            return r;
        }
        public static Domain operator +(Domain et, (long, object) x)
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
            return (Domain)et.New(m + x);
        }
        public static Domain operator +(Domain et, (Context, long, object) x)
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
            return (Domain)et.New(m + (dp, ob));
        }
        public static Domain operator-(Domain d,long x)
        {
            var rp = CTree<long, Domain>.Empty;
            var m = d.mem-x;
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
            var rt = BList<long?>.Empty;
            for (var b=d.rowType.First();b is not null;b=b.Next())
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
        internal virtual (BList<long?>, CTree<long, Domain>, BList<long?>, BTree<long, long?>, BTree<string, (int, long?)>)
ColsFrom(Context cx, long dp, BList<long?> rt, CTree<long, Domain> rs, BList<long?> sr, BTree<long, long?> tr,
 BTree<string, (int, long?)> ns)
        {
            if (super is Table st)
                (rt, rs, sr, tr, ns) = st.ColsFrom(cx, dp, rt, rs, sr, tr, ns);
            var j = 0;
            for (var b = rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && (cx.obs[p]??representation[p]) is SqlValue tc)
                {
                    var nc = new SqlCopy(cx.GetUid(), cx, tc.name??tc.NameFor(cx), dp, tc,
                        BTree<long, object>.Empty + (SqlValue.SelectDepth, cx.sD));
                    nc = (SqlCopy)cx.Add(nc);
                    rt = Add(rt, -1, nc.defpos, tc.defpos);
                    sr = Add(sr, -1, tc.defpos, tc.defpos);
                    rs += (nc.defpos, nc.domain);
                    tr += (tc.defpos, nc.defpos);
                    ns += (nc.alias ?? nc.name ?? "", (j++, nc.defpos));
                }
            return (rt, rs, sr, tr, ns);
        }
        internal virtual (Table, BList<long?>, CTree<long, Domain>)
            ColsFrom(Context cx,BList<long?> rt, CTree<long, Domain> rs)
        {
            Table r;
            if (super is Table st)
                (r, rt, rs) = st.ColsFrom(cx, rt, rs);
            for (var b = rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && representation[p] is Domain cd)
                {
                    rt += p;
                    rs += (p, cd);
                }
            return (new Table(-1L,cx,rs,rt,rt.Length), rt, rs);
        }
        /// <summary>
        /// We take a lot of trouble to ensure Id, LEAVING and ARRIVING are in positions 0,1,2
        /// </summary>
        /// <param name="a">Current rowType</param>
        /// <param name="k">A suggested seq position</param>
        /// <param name="v">The value to insert</param>
        /// <param name="p">The tablecolumn (might not be the same as v)</param>
        /// <returns></returns>
        internal virtual BList<long?> Add(BList<long?> a, int k, long v, long p)
        {
            while (k > a.Length)
                a += -1L;
            if (k < 0 || k==a.Length)
                return a + v;
            var r = BList<long?>.Empty;
            var was = -1L;
            for (var b = a.First(); b != null; b = b.Next())
            {
                var w = b.value() ?? -1L;
                if (b.key() == k)
                {
                    r += v;
                    if (w != v && w >= 0L)
                        was = w;
                }
                else if (w>=0)
                    r += w;
            }
            if (was >= 0L)
                r += was;
            return r;
        }
        public long? this[int i] => (i>=0 && i<rowType.Length)?rowType[i]:-1L;
        public ABookmark<int,long?>? First()
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
            var d0 = this - NotNull - Descending;
            if (wr.cx.db.Find(d0) is Domain d && (d0.CompareTo(d)==0 || d.defpos<Transaction.TransPos))
                return d.defpos;
            if (wr.cx.role.dbobjects[NameFor(wr.cx)] is long p)
                return p; 
            Physical pp;
            pp = new PDomain(this, wr.Length, wr.cx);
            (_,pp) = pp.Commit(wr, tr);
            return pp.ppos;
        }
        internal virtual CTree<long, bool> Needs(Context cx, long r, CTree<long, bool> s)
        {
            for (var b = rowType?.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue sv)
                    s = sv.Needs(cx, r, s);
            return s;
        }
        internal bool Match(string n)
        {
            for (var d = this; d != null; d = (d as UDType)?.super)
                if (d.name == n)
                    return true;
            return false;
        }
        internal Domain SourceRow(Context cx,long dp)
        {
            var rs = CTree<long, Domain>.Empty;
            for (var b = rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue sv)
                {
                    if (sv is SqlFunction sf && sf.IsAggregation(cx,CTree<long,bool>.Empty)!=CTree<long,bool>.Empty)
                    {
                        if (cx.obs[sf.val] is SqlValue v) rs += (v.defpos, v.domain);
                        if (cx.obs[sf.op1] is SqlValue w) rs += (w.defpos, w.domain);
                        if (cx.obs[sf.op2] is SqlValue x) rs += (x.defpos, x.domain);
                        cx.Add(sf + (_From, dp));
                    }
                    else
                        rs += (sv.defpos, sv.domain);
                }
            var rt = BList<long?>.Empty;
            for (var b = rs.First(); b != null; b = b.Next())
                rt += b.key();
            return new Domain(-1L, cx, Sqlx.ROW, rs, rt);
        }
        internal virtual void Show(StringBuilder sb)
        {
            if (defpos >= 0)
            {
                sb.Append(' '); sb.Append(Uid(defpos));
            }
            if (name != "")
            {
                sb.Append(' '); sb.Append(name);
            }
            if (kind != Sqlx.NO)
            {
                sb.Append(' '); sb.Append(kind);
            }
            if (mem.Contains(RowType))
            {
                var cm = " (";
                for (var b = rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = (b.key() + 1 == display) ? "|" : ",";
                        sb.Append(Uid(p));
                        sb.Append(' ');
                        if (representation[p] is Domain dm)
                        {
                            if (dm.name != "")
                                sb.Append(dm.name);
                            else
                                sb.Append(dm.kind);
                        }
                    }
                sb.Append(')');
            }
            if (aggs.Count>0)
            {
                var cm = " Aggs (";
                for (var b = aggs.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                sb.Append(')');
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
            if (defaultValue!=TNull.Value && this is not UDType)
            { sb.Append(" Default="); sb.Append(defaultValue); }
            if (mem.Contains(Element))
            { sb.Append(" elType="); sb.Append(elType); }
            if (mem.Contains(End)) { sb.Append(" End="); sb.Append(end); }
            // if (mem.Contains(Names)) { sb.Append(' '); sb.Append(names); } done in Columns
            if (mem.Contains(_OrderCategory) && orderflags != OrderCategory.None
                && orderflags!=OrderCategory.Primitive)
            { sb.Append(' '); sb.Append(orderflags); }
            if (mem.Contains(OrderFunc)) { sb.Append(" OrderFunc="); sb.Append(orderFunc); }
            if (mem.Contains(Precision) && prec != 0) { sb.Append(" Prec="); sb.Append(prec); }
            if (mem.Contains(Scale) && scale != 0) { sb.Append(" Scale="); sb.Append(scale); }
            if (mem.Contains(Start)) { sb.Append(" Start="); sb.Append(start); }
            if (AscDesc == Sqlx.DESC) sb.Append(" DESC");
            if (nulls != Sqlx.NULL) sb.Append(" " + nulls);
        }
        /// <summary>
        /// API development support: generate the C# type information for a field 
        /// </summary>
        /// <param name="dt">The type to use</param>
        /// <param name="db">The database</param>
        /// <param name="sb">a string builder</param>
        internal void DisplayType(Context cx, StringBuilder sb)
        {
            var i = 0;
            for (var b = representation.First(); b != null; b = b.Next(), i++)
            {
                var p = b.key();
                var cd = b.value();
                if (cx._Ob(p) is not DBObject c || c.NameFor(cx) is not string n)
                    throw new DBException("42105");
                string tn = "";
                if (cd.kind != Sqlx.TYPE && cd.kind != Sqlx.ARRAY && cd.kind != Sqlx.MULTISET)
                    tn = cd.SystemType.Name;
                if (cd.kind == Sqlx.ARRAY || cd.kind == Sqlx.MULTISET)
                {
                    if (tn == "[]")
                        tn = "_T" + i + "[]";
                    if (n.EndsWith("("))
                        n = "_F" + i;
                }
                cd.FieldType(cx, sb);
                sb.Append("  public " + tn + " " + n + ";\r\n");
            }
            for (var b = representation.First(); b != null; b = b.Next(), i++)
            {
                var p = b.key();
                var cd = b.value();
                if ((cd.kind != Sqlx.ARRAY && cd.kind != Sqlx.MULTISET && cd.kind!= Sqlx.SET) || cd.elType==null)
                    continue;
                cd = cd.elType;
                var tn = cx.NameFor(p);
                if (tn != null)
                    sb.Append("// Delete this declaration of class " + tn + " if your app declares it somewhere else\r\n");
                else
                    tn += "_T" + i;
                sb.Append("  public class " + tn + " {\r\n");
                cd.DisplayType(cx, sb);
                sb.Append("  }\r\n");
            }
        }
        /// <summary>
        /// A readable version of the Domain
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var tn = GetType().Name;
            if (this is StandardDataType||this is NodeType)
                tn = "";
            var sb = new StringBuilder(tn);
            if (name != "")
            {
                if (tn!="") sb.Append(' '); 
                sb.Append(name);
            }
            if (kind != Sqlx.NO)
            {
                sb.Append(' '); sb.Append(kind);
            }
            if (mem.Contains(RowType))
            {
                var cm = " (";
                for (var b = rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = (b.key() + 1 == display) ? "|" : ",";
                        sb.Append(Uid(p));
                    }
                if (rowType != BList<long?>.Empty)
                    sb.Append(')');
            }
            _Show(sb);
            if (mem.Contains(Representation))
            {
                var cm = "";
                if (mem.Contains(RowType) && rowType.Length == representation.Count)
                {
                    for (var b = rowType.First(); b != null; b = b.Next())
                        if (b.value() is long p)
                        {
                            sb.Append(cm); cm = ",";
                            sb.Append("[" + Uid(p) + "," + representation[p] + "]");
                        }
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
                    sb.Append(')');
            }
            if (mem.Contains(Constraints))
            { 
                var cm = " constraints=("; 
                for (var b=constraints.First();b is not null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                if (cm == ",") sb.Append(')');
            }
            if (notNull)
                sb.Append(" NOT NULL");
            return sb.ToString();
        }
        internal static Sqlx Equivalent(Sqlx kind)
        {
            return kind switch
            {
                Sqlx.NCHAR or Sqlx.CLOB or Sqlx.NCLOB or Sqlx.VARCHAR => Sqlx.CHAR,
                Sqlx.INT or Sqlx.BIGINT or Sqlx.POSITION or Sqlx.SMALLINT => Sqlx.INTEGER,
                Sqlx.DECIMAL or Sqlx.DEC => Sqlx.NUMERIC,
                Sqlx.DOUBLE or Sqlx.FLOAT => Sqlx.REAL,
                //        case Sqlx.TABLE: return Sqlx.ROW; not equivalent!
                _ => kind,
            };
        }
        internal bool CanBeAssigned(object o)
        {
            if (o == null)
                return true;
            if (o is TypedValue v)
                return v.dataType.EqualOrStrongSubtypeOf(this)
                    || CanBeAssigned(v);
            switch (Equivalent(kind))
            {
                case Sqlx.PASSWORD:
                case Sqlx.CHAR: 
                    if (o is string so)
                        return prec==0 || prec>=so.Length;
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
        internal virtual int Typecode()
        {
            return Equivalent(kind) switch
            {
                Sqlx.NULL => 0,
                Sqlx.INTEGER => 1,
                Sqlx.NUMERIC => 2,
                Sqlx.REAL => 8,
                Sqlx.CHECK or Sqlx.LEVEL or Sqlx.NCHAR => 3,
                Sqlx.CHAR => 3,
                Sqlx.NODETYPE or Sqlx.EDGETYPE => 3, // a string versioon of the node/edge
                Sqlx.TIMESTAMP => 4,
                Sqlx.DATE => 13,
                Sqlx.BLOB => 5,
                Sqlx.ROW => 6,
                Sqlx.ARRAY => 7,
                Sqlx.SET => 7,
                Sqlx.MULTISET => 7,
                Sqlx.TABLE => 7,
                Sqlx.TYPE => 12,
                Sqlx.BOOLEAN => 9,
                Sqlx.INTERVAL => 10,
                Sqlx.TIME => 11,
                Sqlx.XML => 3,
                Sqlx.PERIOD => 7,
                Sqlx.PASSWORD => 3,
                _ => 0,
            };
        }
        public TypedValue Get(BTree<long,Physical.Type>log,Reader rdr,long pp)
        {
            if (rdr.context == null)
                return TNull.Value;
            if (sensitive)
                return new TSensitive(this, Get(log,rdr,pp));
            switch (Equivalent(kind))
            {
                case Sqlx.NULL: return TNull.Value;
                case Sqlx.Null: return TNull.Value;
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
                            var h = (Delta.Verb)rdr.ReadByte();
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
                        var el = (Domain?)rdr.context.db.objects[dp] ?? throw new DBException("42105");
                        var vs = BList<TypedValue>.Empty;
                        var n = rdr.GetInt();
                        for (int j = 0; j < n; j++)
                            if (GetDataType(rdr) is Domain dt)
                                vs += el.Coerce(rdr.context,dt.Get(log, rdr, pp));
                        return new TList(this, vs);
                    }
                case Sqlx.MULTISET:
                    {
                        var dp = rdr.GetLong();
                        var el = (Domain?)rdr.context.db.objects[dp] ?? throw new DBException("42105");
                        var m = new TMultiset(this);
                        var n = rdr.GetInt();
                        for (int j = 0; j < n; j++)
                             m = m.Add(el.Get(log, rdr, pp));
                        return m;
                    }
                case Sqlx.SET:
                    {
                        var dp = rdr.GetLong();
                        var el = (Domain?)rdr.context.db.objects[dp] ?? throw new DBException("42105");
                        var m = new TSet(this);
                        var n = rdr.GetInt();
                        for (int j = 0; j < n; j++)
                            m = m.Add(el.Get(log, rdr, pp));
                        return m;
                    }
                case Sqlx.REF:
                case Sqlx.ROW:
                case Sqlx.TABLE:
                    {
                        var dp = rdr.GetLong();
                        var vs = CTree<long,TypedValue>.Empty;
                        var dt = CTree<long, Domain>.Empty;
                        var rt = BList<long?>.Empty;
                        var tb = (Table?)rdr.context.db.objects[dp];
                        var ma = BTree<string, long?>.Empty;
                        if (rdr.context.db.format < 52)
                            for (var b = tb?.rowType.First(); b != null; b = b.Next())
                                if (b.value() is long c)
                                {
                                    var tc = (TableColumn?)rdr.context.db.objects[c];
                                    var oi = tc?.infos[rdr.context.role.defpos];
                                    ma += (oi?.name ?? "", c);
                                }
                        var n = rdr.GetInt();
                        for (var j = 0; j < n; j++)
                        {
                            long cp;
                            if (rdr.context.db.format < 52)
                                cp = ma[rdr.GetString()]??-1L;
                            else
                                cp = rdr.GetLong();
                            if (tb?.representation[cp] is Domain cdt)
                            {
                                dt += (cp, cdt);
                                rt += cp;
                                vs += (cp, cdt.Get(log, rdr, pp));
                            }
                        }
                        return new TRow(new Domain(rdr.context.GetUid(),rdr.context,Sqlx.ROW,dt,rt), vs);
                    }
                case Sqlx.TYPE:
                    {
                        var dp = rdr.GetLong();
                        var ut = (UDType)(rdr.context.db.objects[dp] ?? throw new DBException("42105"));
                        ut.Instance(dp, rdr.context, null);
                        if (ut.superShape == true)
                            return new TSubType(ut, ut.super?.Get(log, rdr, pp) ?? TNull.Value);
                        else
                        {
                            var r = CTree<long, TypedValue>.Empty;
                            var ma = BTree<string, long?>.Empty;
                            if (rdr.context.db.format < 52)
                                for (var b = ut.rowType.First(); b != null; b = b.Next())
                                    if (b.value() is long c)
                                    {
                                        var tc = (TableColumn?)rdr.context.db.objects[c];
                                        var oi = tc?.infos[rdr.role.defpos];
                                        ma += (oi?.name ?? "", c);
                                    }
                            var n = rdr.GetInt();
                            for (var j = 0; j < n; j++)
                            {
                                long cp;
                                if (rdr.context.db.format < 52)
                                    cp = ma[rdr.GetString()] ?? -1L;
                                else
                                    cp = rdr.GetLong();
                                if (ut.representation[cp] is Domain cdt)
                                    r += (cp, cdt.Get(log, rdr, pp));
                            }
                            return new TRow(ut, r);
                        }
                    }
                case Sqlx.CONTENT:
                    return new TChar(rdr.GetString());
            }
            throw new DBException("3D000").ISO();
        }
        internal int PosFor(Context cx, string c)
        {
            for (var b = rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && cx.NameFor(p) == c)
                    return b.key();
            return -1;
        }
        public Domain? GetDataType(Reader rdr)
        {
            var b = (DataType)rdr.ReadByte();
            if (b == DataType.Null)
                return null;
            if (b == DataType.DomainRef)
                return (Domain?)rdr.context.db.objects[rdr.GetLong()];
            return b switch
            {
                DataType.Null => Null,
                DataType.TimeStamp => Timestamp,
                DataType.Interval => Interval,
                DataType.Integer => Int,
                DataType.Numeric => _Numeric,
                DataType.String => Char,
                DataType.Date => Date,
                DataType.TimeSpan => Timespan,
                DataType.Boolean => Bool,
                DataType.Blob => Blob,
                DataType.Row => Row,
                DataType.Password => Password,
                _ => this,
            };
        }
        public static Domain For(Sqlx dt)
        {
            return Equivalent(dt) switch
            {
                Sqlx.CHAR => Char,
                Sqlx.TIMESTAMP => Timestamp,
                Sqlx.INTERVAL => Interval,
                Sqlx.INT => Int,
                Sqlx.NUMERIC => _Numeric,
                Sqlx.DATE => Date,
                Sqlx.TIME => Timespan,
                Sqlx.BOOLEAN => Bool,
                Sqlx.BLOB => Blob,
                Sqlx.ROW => Row,
                Sqlx.MULTISET => Multiset,
                Sqlx.ARRAY => Array,
                Sqlx.PASSWORD => Password,
                Sqlx.TABLE => TableType,
                Sqlx.TYPE => TypeSpec,
                Sqlx.POSITION => Position,
                _ => Null,
            };
        }
        public virtual Domain For()
        {
            return For(kind);
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
            if (EqualOrStrongSubtypeOf(nt) && wr.cx.db.Find(this) is Domain d
                && d.defpos<Transaction.TransPos && CompareTo(nt)!=0)
            {
                wr.WriteByte((byte)DataType.DomainRef);
                wr.PutLong(d.defpos);
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
                    case Sqlx.SET: 
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
                        var nd = (Domain?)wr.cx.db.objects[defpos]??Content; // without names
                        wr.PutLong(wr.cx.db.Find(nd)?.defpos ??Content.defpos); break;
                    case Sqlx.REF:
                    case Sqlx.ROW: wr.WriteByte((byte)DataType.Row); break;
                }
        }
        public virtual void Put(TypedValue tv, Writer wr)
        {
            switch (Equivalent(kind))
            {
                case Sqlx.SENSITIVE: elType?.Put(tv, wr); return;
                case Sqlx.NULL: return;
                case Sqlx.BLOB: wr.PutBytes(((TBlob)tv).value); return;
                case Sqlx.BOOLEAN: wr.WriteByte((byte)((tv.ToBool() is bool b && b) ? 1 : 0)); return;
                case Sqlx.CHAR: wr.PutString(tv.ToString()); return;
                case Sqlx.DOCUMENT:
                    {
                        if (tv is TDocument d)
                            wr.PutBytes(d.ToBytes(null));
                        return;
                    }
                case Sqlx.INCREMENT:
                    {
                        if (tv is not Delta d)
                            return;
                        wr.PutInt((int)d.details.Count);
                        for (var db = d.details.First(); db != null; db = db.Next())
                        {
                            var de = db.value();
                            wr.PutInt(de.ix);
                            wr.WriteByte((byte)de.how);
                            wr.PutString(de.name.ToString());
                            de.what.dataType.Put(de.what, wr);
                        }
                        return;
                    }
#if MONGO
                case Sqlx.OBJECT: PutBytes(p, ((TObjectId)v).ToBytes()); break;
#endif
                case Sqlx.DOCARRAY:
                    {
                        if (tv is TDocArray d)
                            wr.PutBytes(d.ToBytes());
                        return;
                    }
                case Sqlx.PASSWORD: goto case Sqlx.CHAR;
#if SIMILAR
                case Sqlx.REGULAR_EXPRESSION: goto case Sqlx.CHAR;
#endif
                case Sqlx.XML: goto case Sqlx.CHAR;
                case Sqlx.INTEGER:
                    {
                        if (tv is TInteger n)
                            wr.PutInteger(n.ivalue);
                        else if (tv.ToLong() is long xi)
                            wr.PutLong(xi);
                        return;
                    }
                case Sqlx.NUMERIC:
                    {
                        Numeric? d = null;
                        if (tv is TNumeric tn)
                            d = tn.value;
                        if (tv is TInt && tv.ToLong() is long xn)
                            d = new Numeric(xn);
                        if (tv is TInteger it)
                            d = new Numeric(it.ivalue, 0);
                        if (d is null)
                            throw new PEException("PE49003");
                        wr.PutInteger(d.mantissa);
                        wr.PutInt(d.scale);
                        return;
                    }
                case Sqlx.REAL:
                    {
                        Numeric d;
                        if (tv == null)
                            return;
                        if (tv is TReal)
                            d = new Numeric(tv.ToDouble());
                        else if (tv is TNumeric na)
                            d = na.value;
                        else break;
                        wr.PutInteger(d.mantissa);
                        wr.PutInt(d.scale);
                        return;
                    }
                case Sqlx.DATE:
                    {
                        if (tv is TInt && tv.ToLong() is long x)
                            wr.PutLong(x);
                        else if (tv is TDateTime td && td.value is DateTime dt)
                            wr.PutLong(dt.Ticks);
                        return;
                    }
                case Sqlx.TIME:
                    {
                        if (tv is TInt && tv.ToLong() is long xt)
                            wr.PutLong(xt);
                        else if (tv is TTimeSpan ts && ts.value is TimeSpan st)
                            wr.PutLong(st.Ticks);
                        return;
                    }
                case Sqlx.TIMESTAMP:
                    {
                        if (tv is TInt && tv.ToLong() is long xs)
                            wr.PutLong(xs);
                        else if (tv is TDateTime te && te.value is DateTime de)
                            wr.PutLong(de.Ticks); 
                        return;
                    }
                case Sqlx.INTERVAL:
                    {
                        Interval n;
                        if (tv is TInt ti && ti.value is long tl) // shouldn't happen!
                            n = new Interval(tl);
                        else
                            n = ((TInterval)tv).value;
                        wr.WriteByte(n.yearmonth ? (byte)1 : (byte)0);
                        if (n.yearmonth)
                        {
                            wr.PutInt(n.years);
                            wr.PutInt(n.months);
                        }
                        else
                            wr.PutLong(n.ticks);
                        return;
                    }
                case Sqlx.ROW:
                    {
                        if (tv is TList ta)
                        {
                            if (ta.Length >= 1)
                                tv = ta[0];
                            else
                                return;
                        }
                        tv = Coerce(wr.cx, tv);
                        if (tv is not TRow rw)
                            return;
                        wr.PutLong(defpos);
                        var st = rw.dataType;
                        var cs = rw.columns;
                        wr.PutInt(cs.Length);
                        for (var rb = cs.First(); rb != null; rb = rb.Next())
                            if (rb.value() is long p)
                            {
                                if (wr.cx.db.format >= 52)
                                    wr.PutLong(p);
                                else
                                {
                                    var n = NameFor(wr.cx, p, rb.key());
                                    wr.PutString(n);
                                }
                                st.representation[p]?.Put(rw[p], wr);
                            }
                        return;
                    }
                case Sqlx.TYPE:
                    {
                        wr.PutLong(defpos);
                        tv = Coerce(wr.cx, tv);
                        if (tv is TSubType si)
                        {
                            var sd = si.value.dataType;
                            sd.Put(si.value, wr);
                        }
                        else if (tv is TRow rw)
                        {
                            var st = rw.dataType;
                            var rs = st.representation;
                            wr.PutInt((int)rs.Count);
                            var j = 0;
                            for (var bb = rs.First(); bb != null; bb = bb.Next(), j++)
                            {
                                var p = bb.key();
                                if (wr.cx.db.format >= 52)
                                    wr.PutLong(p);
                                else
                                {
                                    var n = NameFor(wr.cx, p, j);
                                    wr.PutString(n);
                                }
                                bb.value().Put(rw[p], wr);
                            }
                        }
                        return;
                    }
                case Sqlx.REF: goto case Sqlx.ROW;
                case Sqlx.ARRAY:
                    {
                        var a = (TList)tv;
                        var et = a.dataType.elType ?? throw new PEException("PE50708");
                        wr.PutLong(wr.cx.db.Find(et)?.defpos ?? throw new PEException("PE48814"));
                        wr.PutInt(a.Length);
                        for(var ab=a.list.First();ab is not null;ab=ab.Next())
                            et.Put(ab.value(), wr);
                        return;
                    }
                case Sqlx.MULTISET:
                    {
                        TMultiset m = (TMultiset)tv;
                        var et = m.dataType.elType?? throw new PEException("PE50706");
                        wr.PutLong(wr.cx.db.Find(et)?.defpos ?? throw new PEException("PE48815"));
                        wr.PutInt((int)m.Count);
                        for (var a = m.tree.First(); a != null; a = a.Next())
                            for (int i = 0; i < a.value(); i++)
                                et.Put(a.key(), wr);
                        return;
                    }
                case Sqlx.SET:
                    {
                        TSet m = (TSet)tv;
                        var et = m.dataType.elType ?? throw new PEException("PE50707");
                        wr.PutLong(wr.cx.db.Find(et)?.defpos ?? throw new PEException("PE48815"));
                        wr.PutInt((int)m.tree.Count);
                        for (var a = m.tree.First(); a != null; a = a.Next())
                            et.Put(a.key(), wr);
                        return;
                    }
                case Sqlx.METADATA:
                    {
                        var m = (TMetadata)tv;
                        wr.PutString(m.ToString());
                        return;
                    }
            }
        }
        protected static int Comp(IComparable? a,IComparable? b)
        {
            if (a == null && b == null)
                return 0;
            if (a == null)
                return -1;
            if (b == null)
                return 1;
            return a.CompareTo(b);
        }
        public virtual int CompareTo(object? obj)
        {
            if (obj == null)
                return 1;
            var that = (Domain)obj;
            var c = kind.CompareTo(that.kind);
            if (c != 0)
                return c;
            c = Comp(AscDesc, that.AscDesc);
            if (c != 0)
                return c;
            c = Comp(notNull, that.notNull);
            if (c != 0) 
                return c;
            c = Comp(name, that.name); // definer's name
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
            if ((kind == Sqlx.ARRAY || kind == Sqlx.SET || kind == Sqlx.MULTISET) && that.kind == kind && elType is not null)
            {
                c = elType.CompareTo(that.elType);
                if (c != 0)
                    return c;
            }
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
                case Sqlx.NODETYPE:
                case Sqlx.EDGETYPE:
                    return Context.Compare(rowType,that.rowType);
                case Sqlx.TYPE:
                    if (name is string nm)
                        return nm.CompareTo(that.name);
                    else if (that.name is not null)
                        return -1;
                    else return 0;
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
            if (a==b)
                return 0;
            if (a == TNull.Value) // repeat the test to keep compiler happy
                return (nulls == Sqlx.FIRST) ? 1 : -1;
            if (b == TNull.Value)
                return (nulls == Sqlx.FIRST) ? -1 : 1;
            if (a is TSet ax && b is not TSet && ax.Cardinality() == 1)
                a = ax.First()?.Value()??TNull.Value;
            if (b is TSet ay && a is not TSet && ay.Cardinality() == 1)
                b = ay.First()?.Value() ?? TNull.Value;
            int c=0;
            if (orderflags != OrderCategory.None && orderflags != OrderCategory.Primitive
                && orderFunc is not null)
            {
                var cx = Context._system;
                var sa = new SqlLiteral(cx.GetUid(),a);
                cx.Add(sa);
                var sb = new SqlLiteral(cx.GetUid(),b);
                cx.Add(sb);
                if ((orderflags & OrderCategory.Relative) == OrderCategory.Relative)
                {
                    orderFunc.Exec(cx, new BList<long?>(sa.defpos) + sb.defpos);
                    if (cx.val.ToInt() is int ri)
                        c = ri;
                    else
                        throw new DBException("22004");
                    goto ret;
                }
                orderFunc.Exec(cx,new BList<long?>(sa.defpos));
                a = cx.val;
                orderFunc.Exec(cx,new BList<long?>(sb.defpos));
                b = cx.val;
                c = a.dataType.Compare(a, b);
                goto ret;
            }
            switch (Equivalent(kind))
            {
                case Sqlx.BOOLEAN:
                    {
                        if (a?.ToBool() is bool ab)
                            c = ab.CompareTo(b?.ToBool());
                        else c = (b == null) ? 0 : -1;
                        break;
                    }
                case Sqlx.CHAR:
                    goto case Sqlx.CONTENT;
                case Sqlx.XML: goto case Sqlx.CHAR;
                case Sqlx.INTEGER:
                    if (a is TInteger ai)
                    {
                        if (b is TInteger bi)
                            c = ai.ivalue.CompareTo(bi.ivalue);
                        else if (b is TInt ib)
                            c = ai.ivalue.CompareTo(new Integer(ib.value));
                    } else if (a is TInt ia)
                    {
                        if (b is TInteger bi)
                            c = new Integer(ia.value).CompareTo(bi.ivalue);
                        else if (b is TInt ib)
                            c = ia.value.CompareTo(ib.value);
                    }
                    break;
                case Sqlx.NUMERIC:
                    {
                        if (a is TNumeric an && b is TNumeric bn)
                            c = an.value.CompareTo(bn.value);
                    }
                    break;
                case Sqlx.REAL:
                        c = a.ToDouble().CompareTo(b.ToDouble());
                    break;
                case Sqlx.DATE:
                    {
                        if (a is TDateTime da && b is TDateTime db)
                            c = da.value.Ticks.CompareTo(db.value.Ticks);
                        break;
                    }
                case Sqlx.DOCUMENT:
                    {
                        if (a is not TDocument dcb)
                            break;
                        c = dcb.RowSet(b);
                        break;
                    }
                case Sqlx.CONTENT: c = a.ToString().CompareTo(b.ToString()); break;
                case Sqlx.TIME:
                    {
                        if (a is TTimeSpan ta && b is TTimeSpan tb)
                            c = ta.value.CompareTo(tb.value);
                        break;
                    }
                case Sqlx.TIMESTAMP:
                    goto case Sqlx.DATE;
                case Sqlx.INTERVAL:
                    {
                        if (a is TInterval ta && b is TInterval tb)
                            c = ta.value.CompareTo(tb.value);
                        break;
                    }
                case Sqlx.ARRAY:
                    {
                        if (a is TList x && b is TList y)
                        {
                            var xe = x.dataType.elType
                                ?? throw new DBException("22202").Mix().AddValue(y.dataType);
                            if (x.dataType.elType != y.dataType.elType)
                                throw new DBException("22202").Mix()
                                    .AddType(xe).AddValue(y.dataType); 
                            c = 0;
                            for (int j = 0; ; j++)
                            {
                                if (j == x.Length && j == y.Length) break;
                                else if (j == x.Length) c = -1;
                                else if (j == y.Length) c = 1;
                                else c = xe.Compare(x[j], y[j]);
                                if (c != 0)
                                    break;
                            }
                            break;
                        }
                        if (a is TArray tx && b is TArray ty)
                        {
                            var xe = tx.dataType.elType
                                ?? throw new DBException("22202").Mix().AddValue(ty.dataType);
                            if (tx.dataType.elType != ty.dataType.elType)
                                throw new DBException("22202").Mix()
                                    .AddType(xe).AddValue(ty.dataType);
                            c = 0;
                            var xb = tx.array.First();
                            var yb = ty.array.First();
                            for (; xb!=null && yb!=null;xb=xb.Next(),yb=yb.Next())
                            {
                                c = xb.key().CompareTo(yb.key());
                                if (c != 0)
                                    break;
                                c = xe.Compare(xb.value(), yb.value());
                            }
                            if (xb != null)
                                c = 1;
                            if (yb != null)
                                c = -1;
                            break;
                        }
                        else
                            throw new DBException("22004").ISO();
                    }
                case Sqlx.SET:
                    {
                        if (elType is null)
                            break;
                        if (a is not TSet && elType.CanTakeValueOf(a.dataType))
                            a = new TSet(this, new CTree<TypedValue, bool>(a, true));
                        if (b is not TSet && elType.CanTakeValueOf(b.dataType))
                            b = new TSet(this, new CTree<TypedValue, bool>(b, true));
                        if (a is not TSet x || b is not TSet y)
                            throw new DBException("22004").ISO();
                        if (x.dataType.elType == null || y.dataType.elType == null)
                            throw new PEException("PE50705");
                        if (x.dataType.elType != y.dataType.elType)
                            throw new DBException("22002").AddType(x.dataType.elType)
                                .AddValue(y.dataType.elType);
                        var e = x.tree.First();
                        var f = y.tree.First();
                        for (; e != null && f != null; e = e.Next(), f = f.Next())
                        {
                            c = x.dataType.elType.Compare(e.key(), f.key());
                            if (c != 0)
                                goto ret;
                        }
                        c = (e == null) ? ((f == null) ? 0 : -1) : 1;
                        break;
                    }
                case Sqlx.MULTISET:
                    {
                        if (elType is null)
                            break;
                        if (a is not TMultiset && elType.CanTakeValueOf(a.dataType))
                            a = new TMultiset(this, new BTree<TypedValue, long?>(a, 1L),1);
                        if (b is not TMultiset && elType.CanTakeValueOf(b.dataType))
                            b = new TMultiset(this, new BTree<TypedValue, long?>(b, 1L),1);
                        if (a is not TMultiset x || b is not TMultiset y)
                            throw new DBException("22004").ISO();
                        if (x.dataType.elType == null || y.dataType.elType == null)
                            throw new PEException("PE50706");
                        if (x.dataType.elType != y.dataType.elType)
                            throw new DBException("22002").AddType(x.dataType.elType)
                                .AddValue(y.dataType.elType);
                        var e = x.tree.First();
                        var f = y.tree.First();
                        for (;e is not null && f is not null;e=e.Next(),f=f.Next())
                        {
                            c = x.dataType.elType.Compare(e.key(), f.key());
                            if (c != 0)
                                goto ret;
                            c = Context.Compare(e.value(),f.value());
                            if (c != 0)
                                goto ret;
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
                case Sqlx.NODETYPE:
                case Sqlx.EDGETYPE:
                    {
                        if (a is TNode na && b is TNode nb
                            && a.dataType is NodeType ta && b.dataType is NodeType tb)
                        {
                            c = ta.defpos.CompareTo(tb.defpos);
                            if (c != 0)
                                break;
                            if (na.tableRow.vals[ta.idCol] is TInt ia
                            && nb.tableRow.vals[tb.idCol] is TInt ib)
                            {
                                c = ia.CompareTo(ib);
                                break;
                            }
                        }
                        throw new DBException("22004").ISO();
                    }
                case Sqlx.ROW:
                    {
                        if (a is TRow ra && b is TRow rb)
                        {
                            c = 0;
                            var bb = rb.dataType.rowType.First();
                            var cb = ra.dataType.rowType.First();
                            for (; c == 0 && cb != null && bb != null; cb = cb.Next(), bb = bb.Next())
                                if (cb.value() is long p)
                                {
                                    var k = cb.key();
                                    var dm = ra.dataType.representation[p] ?? throw new PEException("PE1289");
                                    c = dm.Compare(ra[k], rb[k]);
                                    if (c != 0)
                                        goto ret;
                                }
                            c = (cb != null) ? 1 : (bb != null) ? -1 : 0;
                            break;
                        }
                        else
                            throw new DBException("22004").ISO();
                    }
                case Sqlx.PERIOD:
                    {
                        if (a is TPeriod pa && b is TPeriod pb && elType is not null)
                        {
                            c = elType.Compare(pa.value.start, pb.value.start);
                            if (c == 0)
                                c = elType.Compare(pa.value.end, pb.value.end);
                        }
                        break;
                    }
                case Sqlx.UNION:
                    {
                        for (var bb = unionOf.First(); bb != null; bb = bb.Next())
                            if (bb.key() is Domain dt)
                                if (dt.CanBeAssigned(a) && dt.CanBeAssigned(b))
                                    return dt.Compare(a, b);
                        throw new DBException("22202", a?.dataType.ToString()??"??", b?.dataType.ToString()??"??");
                    }
                case Sqlx.PASSWORD:
                    throw new DBException("22202").ISO();
                default:
                    c = a.ToString().CompareTo(b.ToString()); break;
            }
            ret:
            return (AscDesc==Sqlx.DESC)?-c:c;
        }
        internal int Compare(CTree<long,TypedValue> a,CTree<long,TypedValue>b)
        {
            int c = 0;
            for (var bm = rowType.First(); c == 0 && bm != null; bm = bm.Next())
                if (bm.value() is long p)
                {
                    var dm = representation[p] ?? throw new PEException("PE1533");
                    c = dm.Compare(a[p] ?? TNull.Value, b[p] ?? TNull.Value);
                }
            return c;
        }
        /// <summary>
        /// Creator: Add the given array at the end of this
        /// </summary>
        /// <param name="a"></param>
        /// <returns></returns>
        public TList Concatenate(TList a, TList b)
        {
            var r = new TList(this);
            var et = elType;
            var ae = a.dataType.elType;
            var be = b.dataType.elType;
            if (ae != et || ae != be)
                throw new DBException("22102").Mix();
            for(var ab=a.list.First();ab is not null;ab=ab.Next())
                r += ab.value();
            for(var bb = b.list.First();bb is not null;bb=bb.Next())
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
            for (var du = dt; du != null; du = (du as UDType)?.super)
                if (du.name == name)
                    return true;
            if (kind == Sqlx.VALUE || kind == Sqlx.CONTENT)
                return true;
            if (dt.kind == Sqlx.CONTENT || dt.kind == Sqlx.VALUE)
                return kind != Sqlx.REAL && kind != Sqlx.INTEGER && kind != Sqlx.NUMERIC;
            if (defpos==NodeType.defpos && dt is NodeType)
                return true;
            if (defpos==EdgeType.defpos && dt is EdgeType)
                return true;
            if (kind == Sqlx.ANY)
                return true;
            if ((dt.kind == Sqlx.TABLE || dt.kind == Sqlx.ROW) && dt.rowType.Length == 1
                && CanTakeValueOf(dt.representation[dt.rowType[0]??-1L]??Null))
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
                for (var te = dt.rowType.First();
                    c && e != null && te != null && e.key() < display;
                    e = e.Next(), te = te.Next())
                    if (e.value() is long ep && te.value() is long tp)
                    {
                        var d = representation[ep];
                        var td = dt.representation[tp] ?? Null;
                        c = d?.CanTakeValueOf(td) ?? false;
                    }
                if (c)
                    return true;
            }
            var ki = Equivalent(kind);
            var dk = Equivalent(dt.kind);
            return ki switch
            {
                Sqlx.CHAR => true,
                Sqlx.NCHAR => dk == Sqlx.CHAR || dk == ki,
                Sqlx.NUMERIC => dk == Sqlx.INTEGER || dk == ki,
                Sqlx.PASSWORD => dk == Sqlx.CHAR || dk == ki,
                Sqlx.REAL => dk == Sqlx.INTEGER || dk == Sqlx.NUMERIC || dk == ki,
                Sqlx.TYPE => rowType.Length == 0 || CompareTo(dt) == 0,
                Sqlx.ROW => rowType.Length == 0 || CompareTo(dt) == 0,
                Sqlx.TABLE => rowType.Length == 0 || CompareTo(dt) == 0, // generic TABLE etc is ok
                Sqlx.ARRAY => CompareTo(dt) == 0,
                Sqlx.MULTISET => CompareTo(dt) == 0,
#if SIMILAR
                Sqlx.CHAR => dk == Sqlx.REGULAR_EXPRESSION || dk == ki
#endif
                _ => dk == ki
            };
        }
        public virtual bool HasValue(Context cx,TypedValue v)
        {
            if (defpos<0 && orderflags==OrderCategory.Primitive && kind==v.dataType.kind)
                return true;
            if (v is TNull)
                return true;
            if (v is TSensitive st)
            {
                if (sensitive)
                    return HasValue(cx,st.value);
                return false;
            }
            var ki = Equivalent(kind);
            if (ki == Sqlx.NULL || kind == Sqlx.ANY)
                return true;
            if (ki == Sqlx.UNION)
                return mem.Contains(cx.db.Find(v.dataType)?.defpos??-1L);
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
                    if (v is TRow vr){
                        var dt = v.dataType;
                        if (vr == null)
                            return false;
                        if (dt.Length != Length)
                            return false;
                        var b = rowType.First();
                        for (var vb = dt.rowType.First(); b != null && vb != null; b = b.Next(), vb.Next())
                            if (b.value() is long c && vb.value() is long vc)
                            {
                                var vd = dt.representation[vc];
                                if (vd == null)
                                    return false;
                                var cd = representation[c] ?? Null;
                                if (!vd.EqualOrStrongSubtypeOf(cd))
                                    return false;
                            }
                        break;
                    }
                    return false;
            }
            return true;
        }
        public virtual TypedValue Parse(long off,string s,Context cx)
        {
            if (s == null)
                return TNull.Value;
            if (sensitive)
                return new TSensitive(this, Parse(new Scanner(off,s.ToCharArray(),0,cx)));
            if (kind == Sqlx.DOCUMENT)
                return new TDocument(s);
            if (kind== Sqlx.METADATA)
                return new TMetadata(new Parser(Database._system,new Connection())
                    .ParseMetadata(s,(int)off,Sqlx.VIEW));
            return Parse(new Scanner(off,s.ToCharArray(), 0, cx));
        }
        public CTree<long, TypedValue> Parse(Context cx, string s)
        {
            var psr = new Parser(cx, s);
            var vs = CTree<long, TypedValue>.Empty;
            if (psr.tok == Sqlx.LPAREN)
                psr.Next();
            // might be named or positional
            var ns = BTree<string, long?>.Empty;
            for (var b = rowType.First(); b != null; b = b.Next())
            if (b.value() is long rp)
                ns += (cx.NameFor(rp), rp);
            for (var j = 0; j < rowType.Length;)
            {
                var a = psr.lxr.val.ToString();
                if (psr.lxr.val is TChar tx && ns.Contains(tx.value))
                {
                    psr.Next();
                    psr.Mustbe(Sqlx.EQL);
                    if (ns[a] is long p &&
                    psr.ParseSqlValueItem(representation[p] ?? Null, false) is SqlLiteral sl)
                        vs += (p, sl.val);
                    else throw new DBException("42000", a);
                }
                else if (rowType[j++] is long rj 
                    && psr.ParseSqlValueItem(representation[rj] ?? Null, false) is SqlLiteral v)
                        vs += (rj, v.val);
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
                return new TSensitive(this, Parse(new Scanner(off, s.ToCharArray(), 0, cx, m)));
            if (kind == Sqlx.DOCUMENT)
                return new TDocument(s);
            return Parse(new Scanner(off,s.ToCharArray(), 0, cx, m));
        }
        /// <summary>
        /// Parse a string value for this type. 
        /// </summary>
        /// <param name="lx">The scanner</param>
        /// <returns>a typedvalue</returns>
        public TypedValue Parse(Scanner lx, bool union = false)
        {
            if (TryParse(lx, out TypedValue v, union) is DBException ex)
                throw ex;
            return v;
        }
        public DBException? TryParse(Scanner lx,out TypedValue v,bool union=false)
        {
            if (lx._cx == null)
                throw new PEException("PE3044");
            v = TNull.Value;
            if (lx.len == 0)
            { v = new TRow(this); return null; }
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
                        var lxr = new Lexer(lx._cx,str);
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
                            str = str[..prec];
                        if (charSet == CharSet.UCS || Check(str))
                        { v = new TChar(str); return null; }
                        break;
                    }
                case Sqlx.CONTENT:
                    {
                        var s = new string(lx.input, lx.pos, lx.input.Length - lx.pos);
                        var i = 1;
                        var c = TDocument.GetValue("", s, s.Length, ref i);
                        lx.pos += i;
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
                                    { v = new TNumeric(this, new Numeric(x, 0)); return null; }
                                    if (kind == Sqlx.REAL)
                                    { v = new TReal(this, (double)x); return null; }
                                    if (lx.ch == '.') // tolerate .00000
                                    {
                                        if (union)
                                            throw new InvalidCastException();
                                        lx.Advance();
                                        if (lx.ch > '5')  // >= isn't entirely satisfactory either
                                            x = (x >= 0) ? x + Integer.One : x - Integer.One;
                                        while (char.IsDigit(lx.ch))
                                            lx.Advance();
                                    }
                                    v = new TInt(this, x); return null;
                                }
                                else
                                {
                                    long x = long.Parse(str);
                                    if (kind == Sqlx.NUMERIC)
                                    { v = new TNumeric(this, new Numeric(x)); return null; }
                                    if (kind == Sqlx.REAL)
                                    { v = new TReal(this, (double)x); return null; }
                                    if (lx.ch == '.') // tolerate .00000
                                    {
                                        //            if (union)
                                        //                throw new InvalidCastException();
                                        lx.Advance();
                                        if (lx.ch > '5') // >= isn't entirely satisfactory either
                                        {
                                            if (x >= 0)
                                                x++;
                                            else
                                                x--;
                                        }
                                        while (char.IsDigit(lx.ch))
                                            lx.Advance();
                                    }
                                    v = new TInt(this, x); return null;
                                }
                            }
                            if ((lx.ch != 'e' && lx.ch != 'E') || kind == Sqlx.NUMERIC)
                            {
                                str = lx.String(start, lx.pos - start);
                                Numeric x = Numeric.Parse(str);
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
                            v = new TReal(this, (double)Numeric.Parse(str));
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
                        if (lx._cx == null)
                            throw new PEException("PE3043");
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
                                if (b.value() is long p && representation[p] is Domain cd)
                                {
                                    var cn = lx._cx.NameFor(p);
                                    TypedValue item = TNull.Value;
                                    if (xc.Attributes != null)
                                    {
                                        var att = xc.Attributes[cn];
                                        if (att != null)
                                            item = cd.Parse(0, att.InnerXml, lx.mime, lx._cx);
                                    }
                                    for (int j = 0; item == null && j < xc.ChildNodes.Count; j++)
                                        if (xc.ChildNodes[j] is XmlNode xn && xn.Name == cn)
                                            item = cd.Parse(0, xn.InnerXml, lx.mime, lx._cx);
                                    blank = blank && (item == null);
                                    cols += (p, item ?? TNull.Value);
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
                                if (b.value() is long p)
                                {
                                    // for this mime type we only understand primitive types
                                    var cd = representation[p] ?? Content;
                                    TypedValue vl = TNull.Value;
                                    try
                                    {
                                        switch (cd.kind)
                                        {
                                            case Sqlx.CHAR:
                                                {
                                                    int st = lx.pos;
                                                    string s;
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
                                                    while (lx.ch != ',' && lx.ch != '\n' && lx.ch != '\r')
                                                        lx.Advance();
                                                    var s = new string(lx.input, st, lx.pos - st);
                                                    if (s.Contains('/'))
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
                                    vs += (p, vl);
                                }
                            v = new TRow(this, vs);
                            return null;
                        }
                        else
                        {
                            if (lx._cx == null)
                                throw new PEException("PE3042");
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
                            var names = BTree<string, long?>.Empty;
                            var b = rowType.First();
                            for (; b != null; b = b.Next())
                                if (b.value() is long p && representation[p] is Domain dm)
                                {
                                    if (lx._cx.obs[p] is SqlValue sv && sv.name is string nm)
                                    {
                                        names += (nm, p);
                                        if (sv.alias != null && sv.alias != "")
                                            names += (sv.alias, p);
                                    }
                                    else
                                    {
                                        var co = lx._cx.NameFor(p); ;
                                        cols += (p, dm.defaultValue);
                                        if (co != null)
                                            names += (co, p);
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
                                if (b.value() is long p)
                                {
                                    if (n == null) // no name supplied
                                    {
                                        if (b == null || !unnamedOk)
                                            return new DBException("22208").Mix();
                                        namedOk = false;
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
                                        if (names[n] is long np && np != p)
                                            p = np;
                                        if (names[n.ToUpper()] is long mp && mp != p)
                                            p = mp;
                                    }
                                    lx.White();
                                    var cv = (representation[p] ?? Content).Parse(lx);
                                    if (n?[0]!='$')
                                        cols += (p, cv);
                                    comma = ',';
                                }
                                lx.White();
                            }
                            if (lx.ch != end && lx.ch!='\0')
                                break;
                            lx.Advance();
                            v = new TRow(this, cols);
                            return null;
                        }
                    }
                case Sqlx.MULTISET:
                case Sqlx.ARRAY:
                    {
                        if (lx._cx == null)
                            throw new PEException("PE3041");
                        v = elType?.ParseList(lx)??TNull.Value;
                        return null;
                    }
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
            if (lx._cx == null)
                throw new PEException("PE3040");
            if (kind == Sqlx.SENSITIVE)
                return new TSensitive(this, elType?.ParseList(lx)??TNull.Value);
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
            return new TList(this, vs);
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
            if (end==Sqlx.NULL || ke < 0)
                ke = ks;
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
            return e switch
            {
                Sqlx.YEAR => 0,
                Sqlx.MONTH => 1,
                Sqlx.DAY => 2,
                Sqlx.HOUR => 3,
                Sqlx.MINUTE => 4,
                Sqlx.SECOND => 5,
                _ => -1,
            };
        }

        /// <summary>
        /// Helper for parts of a date value
        /// </summary>
        /// <param name="lx">the scanner</param>
        /// <param name="n">the number of parts</param>
        /// <returns>n strings</returns>
        static string[] GetParts(Scanner lx, int n, int st)
        {
            string[] r = new string[n];
            for (int j = 0; j < n; j++)
            {
                if (lx.pos > lx.len)
                    throw new DBException("22007", Diag(lx, st)).Mix();
                r[j] = GetPart(lx);
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
        static string GetPart(Scanner lx)
        {
            var st = lx.pos;
            lx.Advance();
            while (char.IsDigit(lx.ch))
                lx.Advance();
            return new string(lx.input, st, lx.pos - st);
        }

        /// <summary>
        /// Get the date part from the string
        /// </summary>
        /// <returns>the DateTime so far</returns>
        static DateTime GetDate(Scanner lx, int st)
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

        static string Diag(Scanner lx, int st)
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
                        f = GetUnsigned(lx);
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
            TimeSpan r = new (h, m, s);
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
        static void GetShortDatePattern(char f, ref char delim, out int delimsBefore, out int len)
        {
            var pat = CultureInfo.CurrentUICulture.DateTimeFormat.ShortDatePattern;
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

        static int GetShortDateField(Scanner lx, char f, ref int pos, int st)
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
        static int GetNDigits(Scanner lx, char delim, int dbef, int n, int st)
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
        static int GetHour(Scanner lx, int st)
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
        static int GetMinutes(Scanner lx, int st)
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
        static int GetSeconds(Scanner lx, int st)
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
        static int GetUnsigned(Scanner lx)
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
        internal virtual TypedValue Coerce(Context cx, TypedValue v)
        {
            if (v == TNull.Value)
                return v;
            if (v is TList ta && ta.Length == 1 && CanTakeValueOf(ta.dataType))
                return Coerce(cx, ta[0]);
            for (var b = constraints?.First(); b != null; b = b.Next())
                if (cx.obs[b.key()]?.Eval(cx.ForConstraintParse()) != TBool.True)
                    throw new DBException("22211");
            if (kind == Sqlx.UNION)
                for (var b = unionOf.First(); b != null; b = b.Next())
                {
                    var du = b.key();
                    if (du.HasValue(cx, v))
                        return v;
                }
            if (kind == Sqlx.COLLECT && elType is not null && v.dataType.EqualOrStrongSubtypeOf(elType))
                return v;
            if (abbrev != "" && v.dataType.kind == Sqlx.CHAR && kind != Sqlx.CHAR)
                v = Parse(new Scanner(-1, v.ToString().ToCharArray(), 0, cx));
            for (var dd = v.dataType; dd != null; dd = (dd as UDType)?.super)
                if (CompareTo(dd) == 0)
                    return v;
            var vk = Equivalent(v.dataType.kind);
            if (Equivalent(kind) != Sqlx.ROW && (vk == Sqlx.ROW || vk == Sqlx.TABLE) && v is TRow rw && rw.Length == 1
                && rw.dataType.rowType.First()?.value() is long p && rw.dataType.representation[p] is Domain di
                && rw.values[p] is TypedValue va)
                return di.Coerce(cx, va);
            if (v.dataType is UDType ut && CanTakeValueOf(ut) && v is TSubType ts)
                return ts.value;
            if (v.dataType.kind == Sqlx.SET && ((TSet)v).Cardinality() == 1)
                return ((TSet)v).First()?.Value() ?? TNull.Value;
            if (v.dataType.kind == Sqlx.MULTISET && ((TMultiset)v).Cardinality() == 1)
                return ((TMultiset)v).First()?.Value() ?? TNull.Value;
            //          if (v.dataType.name == name)
            var kn = Equivalent(kind);
            if (kn == Sqlx.UNION)
                return v;
            switch (kn)
            {
                case Sqlx.INTEGER:
                    {
                        if (vk == Sqlx.INTEGER)
                        {
                            if (prec != 0)
                            {
                                Integer iv;
                                if (v is TInteger vi)
                                    iv = vi.ivalue;
                                else if (v is TInt vv)
                                    iv = new Integer(vv.value);
                                else break;
                                var limit = Integer.Pow10(prec);
                                if (iv >= limit || iv <= -limit)
                                    throw new DBException("22003").ISO()
                                        .AddType(this).AddValue(v);
                                return new TInteger(this, iv);
                            }
                            if (v.ToLong() is long vl)
                                return new TInt(this, vl);
                            if (v.ToInteger() is Integer xv)
                                return new TInteger(this, xv);
                            break;
                        }
                        if (vk == Sqlx.NUMERIC && v is TNumeric a)
                        {
                            var m = a.value.mantissa;
                            var s = a.value.scale;
                            int r = 0;
                            while (s > 0)
                            {
                                m = m.Quotient(10, ref r);
                                s--;
                            }
                            while (s < 0)
                            {
                                m = a.value.mantissa.Times(10);
                                s++;
                            }
                            var na = new Numeric(m, s);
                            if (prec != 0)
                            {
                                var limit = Integer.Pow10(prec);
                                if (na.mantissa >= limit || na.mantissa <= -limit)
                                    throw new DBException("22003").ISO()
                                        .AddType(this).Add(Sqlx.VALUE, v);
                            }
                            return new TInteger(this, na.mantissa);
                        }
                        if (vk == Sqlx.REAL && v.ToLong() is long ii)
                        {
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
                        if (vk == Sqlx.NODETYPE || vk == Sqlx.EDGETYPE)
                            return new TInt(((TNode)v).id);
                    }
                    break;
                case Sqlx.NUMERIC:
                    {
                        Numeric a;
                        if (vk == Sqlx.NUMERIC)
                        {
                            if (v is TNumeric na)
                                a = na.value;
                            else
                                a = new Numeric(v.ToDouble());
                        }
                        else if (v.ToLong() is long ol)
                            a = new Numeric(ol);
                        else if (v is TInteger iv)
                            a = new Numeric(iv.ivalue);
                        else
                            a = new Numeric(v.ToDouble());
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
                        decimal d = new(r);
                        d = Math.Round(d, scale);
                        bool sg = d < 0;
                        if (sg)
                            d = -d;
                        decimal m = 1.0M;
                        for (int j = 0; j < prec - scale; j++)
                            m *= 10.0M;
                        if (d > m)
                            break;
                        if (sg)
                            d = -d;
                        return new TReal(this, (double)d);
                    }
                case Sqlx.DATE:
                    {
                        switch (vk)
                        {
                            case Sqlx.DATE:
                                return v;
                            case Sqlx.CHAR:
                                return new TDateTime(this, DateTime.Parse(v.ToString(),
                                    (cx.conn.props["Locale"] is string lc) ? new CultureInfo(lc)
                                    : v.dataType.culture));
                        }
                        if (v is TDateTime dt)
                            return new TDateTime(this, dt.value);
                        if (v.ToLong() is long lv)
                            return new TDateTime(this, new DateTime(lv));
                        break;
                    }
                case Sqlx.TIME:
                    switch (vk)
                    {
                        case Sqlx.TIME:
                            return v;
                        case Sqlx.CHAR:
                            return new TTimeSpan(this, TimeSpan.Parse(v.ToString(),
                                (cx.conn.props["Locale"] is string lc) ? new CultureInfo(lc)
                                : v.dataType.culture));
                    }
                    break;
                case Sqlx.TIMESTAMP:
                    switch (vk)
                    {
                        case Sqlx.TIMESTAMP: return v;
                        case Sqlx.DATE:
                            return new TDateTime(this, ((TDateTime)v).value);
                        case Sqlx.CHAR:
                            return new TDateTime(this, DateTime.Parse(v.ToString(),
                                (cx.conn.props["Locale"] is string lc) ? new CultureInfo(lc)
                                : v.dataType.culture));
                    }
                    if (v.ToLong() is long vm)
                        return new TDateTime(this, new DateTime(vm));
                    break;
                case Sqlx.INTERVAL:
                    if (v is TInterval zv)
                        return new TInterval(this, zv.value);
                    break;
                case Sqlx.CHAR:
                    {
                        var vt = v.dataType;
                        string str = vt.kind switch
                        {
                            Sqlx.DATE or Sqlx.TIMESTAMP => ((TDateTime)v).value.ToString(culture),
                            Sqlx.CHAR => ((TChar)v).ToString(),
                            Sqlx.CHECK => ((TRvv)v).rvv.ToString(),
                            Sqlx.NODETYPE or Sqlx.EDGETYPE => ((TNode)v).ToString(cx),
                            _ => v.ToString(cx),
                        };
                        if (prec != 0 && str.Length > prec)
                            throw new DBException("22001", "CHAR(" + prec + ")", "CHAR(" + str.Length + ")").ISO()
                                                .AddType(this).AddValue(vt);
                        return new TChar(this, str);
                    }
                case Sqlx.PERIOD:
                    {
                        if (v is not TPeriod pd || elType is null)
                            return TNull.Value;
                        return new TPeriod(this, new Period(elType.Coerce(cx, pd.value.start),
                            elType.Coerce(cx, pd.value.end)));
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
                                    return new TDocument(((TBlob)v).value, ref i);
                                }
                        }
                        return v;
                    }
                case Sqlx.CONTENT: return v;
                case Sqlx.PASSWORD: return v;
                case Sqlx.DOCARRAY: goto case Sqlx.DOCUMENT;
                case Sqlx.TYPE:
                    {
                        if (v is TChar tc)
                        {
                            var s = tc.ToString();
                            if ((this+(Kind,Sqlx.ROW)).TryParse(new Scanner(0, ("("+s+")").ToCharArray(), 0, cx), out v) is DBException e)
                                throw e;
                        }
                        return v;
                    }
                case Sqlx.ROW:
                    {
                        var vs = CTree<long, TypedValue>.Empty;
                        var vb = v.dataType.rowType.First();
                        var d = v.dataType.display;
                        if (d == 0)
                            d = int.MaxValue;
                        var r = (TRow)v;
                        for (var b = rowType.First(); b != null && vb != null && b.key() < d;
                            b = b.Next(), vb = vb.Next())
                            if (b.value() is long pp && vb.value() is long vp &&
                                representation[pp] is Domain dt &&
                                r[vp] is TypedValue tv)
                                vs += (pp, dt.Coerce(cx, tv));
                        if (vb != null)
                            goto bad;
                        return new TRow(this, vs);
                    }
                case Sqlx.VALUE:
                case Sqlx.NULL:
                    return v;
                case Sqlx.ARRAY:
                    if (v is TList && elType is not null && v.dataType.elType is not null
                        && elType.CanTakeValueOf(v.dataType.elType))
                        return v;
                    if (v is TArray && elType is not null && v.dataType.elType is not null
                        && elType.CanTakeValueOf(v.dataType.elType))
                        return v;
                    break;
                case Sqlx.SET:
                    if (v.dataType.elType is not null)
                    {
                        if (v is TSet && elType is not null && elType.CanTakeValueOf(v.dataType.elType))
                            return v;
                        else if (v.dataType.EqualOrStrongSubtypeOf(v.dataType.elType))
                            return new TSet(v.dataType, new CTree<TypedValue, bool>(v, true));
                    }
                    if (elType?.CanTakeValueOf(v.dataType) == true)
                        return new TSet(this, new CTree<TypedValue, bool>(v, true));
                    break;
                case Sqlx.MULTISET:
                    if (v is TMultiset && elType is not null && v.dataType.elType is not null
                        && elType.CanTakeValueOf(v.dataType.elType))
                        return v;
                    if (elType?.CanTakeValueOf(v.dataType) == true)
                        return new TMultiset(v.dataType, new BTree<TypedValue, long?>(v, 1L), 1);
                    break;
                default:
                    return v;
            }
        bad: throw new DBException("22005", this, v.ToString()).ISO();
        }
        /// <summary>
        ///  for accepting Json values
        /// </summary>
        /// <param name="ro"></param>
        /// <param name="ob"></param>
        /// <returns></returns>
        internal TypedValue Coerce(Context cx,object ob)
        {
            if (ob==null)
                return defaultValue;
            if (abbrev != "" && ob is string so && Equivalent(kind) != Sqlx.CHAR)
                return Parse(new Scanner(-1, so.ToCharArray(), 0, cx));
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
                    if (ob is long lo)
                        return new TInt(this, lo);
                    return new TInt(this, (int)ob);
                case Sqlx.NUMERIC:
                    {
                        Numeric nm = new (0);
                        if (ob is long ol)
                            nm = new (ol);
                        if (ob is double od)
                            nm = new (od);
                        if (ob is decimal om)
                            nm = new (om);
                        if (ob is int io)
                            nm = new Numeric(io);
                        return new TNumeric(this, nm);
                    }
                case Sqlx.REAL:
                    {
                        if (ob is decimal om)
                            return new TReal(this, (double)om);
                        if (ob is float fo)
                            return new TReal(this, fo);
                        return new TReal(this, (double)ob);
                    }
                case Sqlx.DATE:
                    return new TDateTime(this, (DateTime)ob);
                case Sqlx.TIME:
                    return new TTimeSpan(this, (TimeSpan)ob);
                case Sqlx.TIMESTAMP:
                    {
                        if (ob is DateTime dt)
                            return new TDateTime(this, dt);
                        if (ob is Date od)
                            return new TDateTime(this, od.date);
                        if (ob is string os)
                            return new TDateTime(this,
                                DateTime.Parse(os, culture));
                        if (ob is long ol)
                            return new TDateTime(this, new DateTime(ol));
                        break;
                    }
                case Sqlx.INTERVAL:
                    if (ob is Interval oi)
                        return new TInterval(this, oi);
                    break;
                case Sqlx.CHAR:
                    {
                        string str = "";
                        if (ob is DateTime od)
                            str = od.ToString(culture);
                        if (ob is Date om)
                            str = om.ToString();
                        if (ob is string os)
                            str = os;
                        if (prec != 0 && str.Length > prec)
                            throw new DBException("22001", "CHAR(" + prec + ")", "CHAR(" + str.Length + ")").ISO()
                                                .AddType(this);
                        return new TChar(this, str);
                    }
                case Sqlx.PERIOD:
                    {
                        var pd = ob as Period ?? throw new DBException("22000");
                        if (elType is null) 
                            throw new DBException("22000");
                        return new TPeriod(this, new Period(elType.Coerce(cx, pd.start),
                            elType.Coerce(cx, pd.end)));
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
                    if (ob is Document d){
                        var vs = CTree<long, TypedValue>.Empty;
                        for (var b = rowType.First(); b != null && b.key()<display; b = b.Next())
                            if (b.value() is long p && cx.obs[p] is SqlValue oc 
                                && oc.NameFor(cx) is string cn)
                            {
                                if (cn == null || cn == "")
                                    cn = "Col" + b.key();
                                var di = cn.IndexOf(".");
                                if (di > 0)
                                    cn = cn[(di + 1)..];
                                var co = (cn != "" && cn != null && d.Contains(cn)) ? d[cn]
                                    : TNull.Value;
                                if (co != null && co != TNull.Value)
                                {
                                    var v = oc.domain.Coerce(cx, co);
                                    vs += (p, v);
                                }
                            }
                        var kb = aggs.First();
                        foreach (var f in d.fields)
                        {
                            if (f.Key == "$check")
                            {
                                vs += (LastChange, new TInt((long)f.Value));
                                vs += (Defpos, new TInt((long)(d["$pos"]??0L)));
                            }
                            if (f.Key == "%classification")
                                vs += (Classification,
                                    TLevel.New(Level.Parse((string)f.Value,cx)));
                            if (f.Key.StartsWith("$#") && kb is not null && cx.obs[kb.key()] is SqlFunction sf)
                            {
                                kb = kb.Next();
                                var key = TRow.Empty;
                                if (cx.groupCols[defpos] is Domain gc && gc!=Null)
                                {
                                    var ks = CTree<long, TypedValue>.Empty;
                                    for (var g = gc.rowType.First(); g != null; g = g.Next())
                                        if (g.value() is long gp && 
                                            cx.obs[gp] is SqlValue kv)
                                        {
                                            var nm = kv.alias ?? kv.NameFor(cx) ?? "";
                                            ks += (gp, kv.domain.Coerce(cx, d[nm]??0L));
                                        }
                                    key = new TRow(gc, ks);
                                    cx.values += key.values;
                                }
                                var fd = (Document)f.Value;
                                var ra = fd.fields.ToArray();
                                var fc = cx.funcs[sf.from]?[key]?[sf.defpos]??sf.StartCounter(cx,key);
                                fc.count += int.Parse(ra[0].Key);
                                switch (sf.op)
                                {
                                    case Sqlx.STDDEV_POP:
                                        fc.acc1 += (double)ra[1].Value;
                                        goto case Sqlx.SUM;
                                    case Sqlx.AVG:
                                    case Sqlx.SUM:
                                        switch (sf.domain.kind)
                                        {
                                            case Sqlx.INTEGER:
                                                {
                                                    var tv = sf.domain.Coerce(cx, ra[0].Value);
                                                    var iv =
                                                    (tv is TInt && tv.ToLong() is long tl) ? 
                                                        new Integer(tl) : ((TInteger)tv).ivalue;
                                                    if (fc.sumInteger is null)
                                                        fc.sumInteger = iv;
                                                    else
                                                        fc.sumInteger += iv;
                                                    fc.sumType = Int;
                                                }
                                                break;
                                            case Sqlx.NUMERIC:
                                                {
                                                    var dv = ((TNumeric)sf.domain.Coerce(cx, ra[0].Value)).value;
                                                    if (fc.sumType != _Numeric || fc.sumDecimal is null)
                                                        fc.sumDecimal = dv;
                                                    else
                                                        fc.sumDecimal += dv;
                                                    fc.sumType = _Numeric;
                                                }
                                                break;
                                            case Sqlx.REAL:
                                                fc.sum1 += sf.domain.Coerce(cx, ra[0].Value).ToDouble();
                                                fc.sumType = Real;
                                                break;
                                        }
                                        break;
                                    case Sqlx.FIRST:
                                        fc.acc ??= sf.domain.Coerce(cx, ra[0].Value);
                                        break;
                                    case Sqlx.LAST:
                                        fc.acc = sf.domain.Coerce(cx, ra[0].Value);
                                        break;
                                    case Sqlx.MAX:
                                        {
                                            var m = sf.domain.Coerce(cx, ra[0].Value);
                                            if (m.CompareTo(fc.acc) > 0)
                                                fc.acc = m;
                                            break;
                                        }
                                    case Sqlx.MIN:
                                        {
                                            var m = sf.domain.Coerce(cx, ra[0].Value);
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
                                            if (fc.mset is null)
                                                throw new PEException("PE1961");
                                            for (int j = 0; j < mm.Length; j++)
                                                fc.mset = fc.mset.Add(sf.domain.Coerce(cx, mm[j].Value));
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
                  //              vs += (sf.defpos, TNull.Value); see case TABLE
                            }
                        }
                        var r = new TRow(this, vs);
                        if (d.Contains("$check")) // used for remote data
                            r += (Rvv.RVV, new TRvv(cx, vs));
                        return r;
                    }
                    break;
                case Sqlx.TABLE:
                    if (ob is DocArray da)
                    {
                        var dr = (Domain)Relocate(cx.GetUid()) + (Kind, Sqlx.ROW);
                        if (cx.groupCols[defpos] is Domain dn)
                            cx.groupCols += (dr.defpos, dn);
                        var va = BList<TypedValue>.Empty;
                        foreach (var o in da.items)
                            va += dr.Coerce(cx, o);
                        return new TList(dr, va);
                    }
                    break;
                case Sqlx.ARRAY:
                    if (ob is DocArray db && elType is not null)
                    {
                        var va = BList<TypedValue>.Empty;
                        foreach (var o in db.items)
                            va += elType.Coerce(cx, o);
                        return new TList(elType, va);
                    }
                    break;
                case Sqlx.SET:
                    if (ob is DocArray ds && elType != null)
                    {
                        var va = CTree<TypedValue, bool>.Empty;
                        foreach (var o in ds.items)
                        {
                            var v = elType.Coerce(cx, o);
                            va += (v, true);
                        }
                        return new TSet(elType, va);
                    }
                    break;
                case Sqlx.MULTISET:
                    if (ob is DocArray dc && elType is not null)
                    {
                        long n = 0;
                        var va = BTree<TypedValue,long?>.Empty;
                        foreach (var o in dc.items)
                        {
                            var v = elType.Coerce(cx, o);
                            var k = va[v] ?? 0L;
                            va += (v, k);
                            n++;
                        }
                        return new TMultiset(elType, va, n);
                    }
                    break;
            }
            throw new DBException("22005", this, ob?.ToString()??"??").ISO();
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
            return Equivalent(t) switch
            {
                Sqlx.BLOB => Blob,
                Sqlx.BLOBLITERAL => Blob,
                Sqlx.BOOLEAN => Bool,
                Sqlx.BOOLEANLITERAL => Bool,
                Sqlx.CHAR => Char,
                Sqlx.CHARLITERAL => Char,
                Sqlx.DATE => Date,
                Sqlx.DOCARRAY => Document,
                Sqlx.DOCUMENT => Document,
         //       Sqlx.DOCUMENTLITERAL => Document,
                Sqlx.INTEGER => Int,
                Sqlx.INTEGERLITERAL => Int,
                Sqlx.INTERVAL => Interval,
                Sqlx.NULL => Null,
                Sqlx.NUMERIC => _Numeric,
                Sqlx.NUMERICLITERAL => _Numeric,
                Sqlx.PASSWORD => Password,
                Sqlx.POSITION => Int,
                Sqlx.REAL => Real,
                Sqlx.REALLITERAL => Real,
                Sqlx.TIME => Timespan,
                Sqlx.TIMESTAMP => Timestamp,
                _ => throw new DBException("42119", t, "CURRENT"),
            };
        }
        /// <summary>
        /// Implementation of the Role$Class table: Produce a type attribute for a field
        /// </summary>
        /// <param name="sb">A string builder to receive the attribute</param>
        /// <param name="dt">The Pyrrho datatype</param>
        internal virtual void FieldType(Context cx, StringBuilder sb)
        {
            var ek = Equivalent(kind);
            sb.Append("[Field(PyrrhoDbType.");
            switch(ek)
            {
                case Sqlx.ARRAY: sb.Append("Array"); break;
                case Sqlx.MULTISET: sb.Append("Multiset"); break;
                case Sqlx.INTEGER: sb.Append("Integer"); break;
                case Sqlx.NUMERIC: sb.Append("Decimal"); break;
                case Sqlx.NCHAR:
                case Sqlx.CHAR: sb.Append("String"); break;
                case Sqlx.REAL: sb.Append("Real"); break;
                case Sqlx.DATE: sb.Append("Date"); break;
                case Sqlx.TIME: sb.Append("Time"); break;
                case Sqlx.INTERVAL: sb.Append("Interval"); break;
                case Sqlx.BOOLEAN: sb.Append("Bool"); break;
                case Sqlx.TIMESTAMP: sb.Append("Timestamp"); break;
                case Sqlx.ROW: sb.Append("Row"); break;
            }
            if (defpos<0) { sb.Append(")]\r\n"); return; }
            if (defpos < Transaction.TransPos)
                sb.Append("," + defpos);
            sb.Append(",\""+ToString()+"\"");
            if ((ek == Sqlx.ARRAY || ek == Sqlx.MULTISET || ek == Sqlx.SET) && elType is not null)
            { sb.Append(','); elType.FieldType(cx, sb); }
            sb.Append(")]\r\n"); 
        }
        /// <summary>
        /// Implementation of the Role$Java table: Produce a type annotation for a field
        /// </summary>
        /// <param name="sb">A string builder to receive the attribute</param>
        /// <param name="dt">The Pyrrho datatype</param>
        internal virtual void FieldJava(Context cx, StringBuilder sb)
        {
            switch (Equivalent(kind))
            {
                case Sqlx.INTEGER:
                    if (prec != 0)
                        sb.Append("@FieldType(PyrrhoDbType.Integer," + prec + ")\r\n");
                    return;
                case Sqlx.NUMERIC:
                    sb.Append("@FieldType(PyrrhoDbType.Decimal," + prec + "," + scale + ")\r\n");
                    return;
                case Sqlx.NCHAR:
                case Sqlx.CHAR:
                    if (prec != 0)
                        sb.Append("@FieldType(PyrrhoDbType.String," + prec + ")\r\n");
                    return;
                case Sqlx.REAL:
                    if (scale != 0 || prec != 0)
                        sb.Append("@FieldType(PyrrhoDBType.Real," + prec + "," + scale + ")\r\n");
                    return;
                case Sqlx.DATE: sb.Append("@FieldType(PyrrhoDbType.Date)\r\n"); return;
                case Sqlx.TIME: sb.Append("@FieldType(PyrrhoDbType.Time)\r\n"); return;
                case Sqlx.INTERVAL: sb.Append("@FieldType(PyrrhoDbType.Interval)\r\n"); return;
                case Sqlx.BOOLEAN: sb.Append("@FieldType(PyrrhoDbType.Bool)\r\n"); return;
                case Sqlx.TIMESTAMP: sb.Append("@FieldType(PyrrhoDbType.Timestamp)\r\n"); return;
                case Sqlx.ROW:
                    if (elType is not null)
                        sb.Append("@FieldType(PyrrhoDbType.Row,"+ elType.name + ")\r\n");
                    return;
            }
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
                return CultureInfo.InvariantCulture;
            n = n.ToLower();
            try
            {
                return n switch
                {
                    "ucs_binary" or "sql_character" or "graphic_irv" or "sql_text" or "sql_identifier" 
                    or "latin1" or "iso8bit" or "unicode" => CultureInfo.InvariantCulture,
                    _ => new CultureInfo(n),
                };
            }
            catch (Exception e)
            {
                throw new DBException("2H000", e.Message).ISO();
            }
        }
        internal override TypedValue _Eval(Context cx)
        {
            if (rowType != BList<long?>.Empty)
            {
                var vs = CTree<long, TypedValue>.Empty;
                for (var b = rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p && cx.obs[p] is SqlValue sv)
                        vs += (p, sv.Eval(cx));
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
            if (kind == Sqlx.TABLE || kind == Sqlx.ROW)
                return Scalar(cx).Eval(lp, cx, a, op, b);
            if (op == Sqlx.NO)
                return Coerce(cx,a);
            if (a is TUnion au)
                a = au.LimitToValue(cx,lp); // a coercion possibly
            if (b is TUnion bu)
                b = bu.LimitToValue(cx,lp);
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
                        a = new TInteger(((TNumeric)a).value.mantissa);
                    if (bk == Sqlx.INTERVAL && kind == Sqlx.TIMES)
                        return Eval(lp,cx,b, op, a);
                    if (bk == Sqlx.NUMERIC)
                        b = new TInteger(((TNumeric)b).value.mantissa);
                    if (ak == Sqlx.INTEGER)
                    {
                        if (a is TInteger ia)
                        {
                            if (b.ToLong() is long lb)
                                return IntegerOps(this, ia.ivalue, op, new Integer(lb));
                            else if (b is TInteger ib)
                                return IntegerOps(this, ia.ivalue, op, ib.ivalue);
                        }  
                        if (a is TInt ai)
                        {
                            if (b is TInteger ib)
                                return IntegerOps(this, new Integer(ai.value), op, ib.ivalue);
                            if (b is TInt  && a.ToLong() is long aa && b.ToLong() is long bb)
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

                    }
                    break;
                case Sqlx.REAL:
                    return new TReal(this, DoubleOps(a.ToDouble(), op, b.ToDouble()));
                case Sqlx.NUMERIC:
                    if (a.dataType.Constrain(cx,lp,Int) != null && a.ToInteger() is Integer vi)
                        a = new TNumeric(new Numeric(vi, 0));
                    if (b.dataType.Constrain(cx,lp,Int) != null && b.ToInteger() is Integer vj)
                        b = new TNumeric(new Numeric(vj, 0));
                    if (a is TNumeric an && b is TNumeric bn)
                        return new TNumeric(DecimalOps(an.value, op, bn.value));
                    var ca = a.ToDouble();
                    var cb = b.ToDouble();
                    return Coerce(cx,new TReal(this, DoubleOps(ca, op, cb)));
                case Sqlx.TIME:
                case Sqlx.TIMESTAMP:
                case Sqlx.DATE:
                    {
                        var ta = ((TDateTime)a).value;
                        switch (bk)
                        {
                            case Sqlx.INTERVAL:
                                {
                                    var ib = ((TInterval)b).value;
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
                                    if (b==TNull.Value)
                                        return TNull.Value;
                                    if (op == Sqlx.MINUS)
                                        return DateTimeDifference(ta, ((TDateTime)b).value);
                                    break;
                                }
                        }
                        throw new DBException("42161", "date operation");
                    }
                case Sqlx.INTERVAL:
                    {

                        if (ak == Sqlx.TIMESTAMP && bk == Sqlx.TIMESTAMP && op == Sqlx.MINUS)
                            return new TInterval(new Interval(((TDateTime)a).value.Ticks - ((TDateTime)b).value.Ticks));
                        else if (ak == Sqlx.DATE && bk == Sqlx.DATE && op == Sqlx.MINUS)
                        {
                            var da = ((TDateTime)a).value;
                            var db = ((TDateTime)b).value;
                            return new TInterval(new Interval(da.Year-db.Year,da.Month-db.Month));
                        }
                        else
                            switch (bk)
                            {
                                case Sqlx.DATE:
                                    return Eval(lp, cx, b, op, a);
                                case Sqlx.INTEGER:
                                    {
                                        var ia = ((TInterval)a).value;
                                        if (b?.ToInt() is int bi && ia.yearmonth)
                                        {
                                            var m = ia.years * 12 + ia.months;
                                            var y = 0;
                                            switch (kind)
                                            {
                                                case Sqlx.TIMES: m *= bi; break;
                                                case Sqlx.DIVIDE: m /= bi; break;
                                            }
                                            if (start == Sqlx.YEAR)
                                            {
                                                y = m / 12;
                                                if (end == Sqlx.MONTH)
                                                    m -= 12 * (m / 12);
                                            }
                                            return new TInterval(this, new Interval(y, m));
                                        }
                                        break;
                                    }
                                case Sqlx.INTERVAL:
                                    {
                                        var ia = ((TInterval)a).value;
                                        var ib = ((TInterval)b).value;
                                        Interval ic;
                                        if (ia.yearmonth != ib.yearmonth)
                                            break;
                                        if (ia.yearmonth)
                                            ic = kind switch
                                            {
                                                Sqlx.PLUS => new Interval(ia.years + ib.years, ia.months + ib.months),
                                                Sqlx.MINUS => new Interval(ia.years - ib.years, ia.months - ib.months),
                                                _ => throw new PEException("PE56"),
                                            };
                                        else
                                            ic = kind switch
                                            {
                                                Sqlx.PLUS => new Interval(ia.ticks - ib.ticks),
                                                Sqlx.MINUS => new Interval(ia.ticks - ib.ticks),
                                                _ => throw new PEException("PE56"),
                                            };
                                        return new TInterval(this, ic);
                                    }
                            }
                        throw new DBException("42161", "date operation");
                    }
                case Sqlx.RDFTYPE:
                    if (elType is not Domain de)
                        break;
                    return Coerce(cx,de.Eval(lp,cx,a, op, b));
                case Sqlx.MULTISET:
                    {
                        if (elType is not Domain me)
                            break;
                        var ms = (TMultiset)a;
                        var e = me.Coerce(cx, b);
                        if (e is not null)
                            return (op==Sqlx.PLUS)?ms.Add(e):ms.Remove(e);
                        break;
                    }
                case Sqlx.SET:
                    {
                        if (elType is not Domain me)
                            break;
                        var ms = (TSet)a;
                        var e = me.Coerce(cx, b);
                        if (e is not null)
                            return (op == Sqlx.PLUS) ? ms.Add(e) : ms.Remove(e);
                        break;
                    }
            }
            throw new DBException("22005", kind, a).ISO();
        }
        /// <summary>
        /// MaxLong bound for knowing if an Integer will fit into a long
        /// </summary>
        static readonly Integer MaxLong = new (long.MaxValue);
        /// <summary>
        /// MinLong bound for knowing if an Integer will fit into a long
        /// </summary>
        static readonly Integer MinLong = new (long.MinValue);
        /// <summary>
        /// Integer operations
        /// </summary>
        /// <param name="a">The left Integer operand</param>
        /// <param name="op">The operator</param>
        /// <param name="b">The right Integer operand</param>
        /// <returns>The Integer result</returns>
        static TypedValue IntegerOps(Domain tp, Integer a, Sqlx op, Integer b)
        {
            Integer r = op switch
            {
                Sqlx.PLUS => a + b,
                Sqlx.MINUS => a - b,
                Sqlx.TIMES => a * b,
                Sqlx.DIVIDE => a / b,
                _ => throw new PEException("PE52"),
            };
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
        static Numeric DecimalOps(Numeric a, Sqlx op, Numeric b)
        {
            var z = Integer.Zero;
            switch (op)
            {
                case Sqlx.PLUS:
                    if (a.mantissa == z)
                        return b;
                    if (b.mantissa == z)
                        return a;
                    return a + b;
                case Sqlx.MINUS:
                    if (a.mantissa == z)
                        return -b;
                    if (b.mantissa == z)
                        return a;
                    return a - b;
                case Sqlx.TIMES:
                    if (a.mantissa == z)
                        return a;
                    if (b.mantissa == z)
                        return b;
                    return a * b;
                case Sqlx.DIVIDE:
                    if (a.mantissa == z)
                        return a;
                    if (b.mantissa == z)
                        return b; // !!
                    return Numeric.Divide(a, b, (a.precision > b.precision) ? a.precision : b.precision);
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
            return op switch
            {
                Sqlx.PLUS => a + b,
                Sqlx.MINUS => a - b,
                Sqlx.TIMES => a * b,
                Sqlx.DIVIDE => a / b,
                _ => throw new PEException("PE54"),
            };
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
            return Equivalent(kind) switch
            {
                Sqlx.Null => 10,
                Sqlx.REAL => 1,
                Sqlx.CHAR => 2,
                Sqlx.DOCUMENT => 3,
                Sqlx.DOCARRAY => 4,
                Sqlx.BLOB => 5,
                Sqlx.OBJECT => 7,
                Sqlx.BOOLEAN => 8,
                Sqlx.TIMESTAMP => 9,
                Sqlx.NULL => 10,
                Sqlx.ROUTINE => 13,
                Sqlx.NUMERIC => 19,// Decimal subtype added for Pyrrho
                Sqlx.INTEGER => 16,
                _ => 6,
            };
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
                    var ts = elType?.Constrain(cx,lp,ce??Null);
                    if (ts is null)
                        return Null;
                    return ts.Equals(elType) ? this : ts.Equals(ce) ? dt :
                        (Domain)cx.Add(new Domain(cx.GetUid(),Sqlx.SENSITIVE, ts));
                }
                var tt = elType?.Constrain(cx,lp,dt);
                if (tt is null)
                    return Null;
                return tt.Equals(elType) ? this : 
                    (Domain)cx.Add(new Domain(cx.GetUid(),Sqlx.SENSITIVE, tt));
            }
            if (dt.kind == Sqlx.SENSITIVE)
            {
                var tu = Constrain(cx,lp,ce??Null);
                if (tu == Null)
                    return Null;
                return tu.Equals(dt.elType) ? dt : 
                    (Domain)cx.Add(new Domain(cx.GetUid(),Sqlx.SENSITIVE, tu));
            }
            if (dt == Null)
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
                return Null;
            if (kind == Sqlx.REAL && dt.kind == Sqlx.INTEGER)
                return Null;
            if (kind == Sqlx.INTERVAL && dt.kind == Sqlx.INTERVAL)
            {
                int s = IntervalPart(start), ds = IntervalPart(dt.start),
                    e = IntervalPart(end), de = IntervalPart(dt.end);
                if (s >= 0 && (s <= 1 != ds <= 1))
                    return Null;
                if (s <= ds && (e >= de || de < 0))
                    return this;
            }
            if (kind == Sqlx.PASSWORD && dt.kind == Sqlx.CHAR)
                return this;
            if (ki == dk && (kind == Sqlx.ARRAY || kind == Sqlx.MULTISET))
            {
                if (elType!=Null)
                    return dt;
                var ect = elType.Constrain(cx,lp,ce??Null);
                if (ect == elType)
                    return this;
                return dt;
            }
            if (ki == Sqlx.UNION && dk != Sqlx.UNION)
                for (var b = unionOf.First(); b != null; b = b.Next())
                    if (b.key() is Domain dm && dm.Constrain(cx,lp,dt).kind != Sqlx.Null)
                        return dm;
            if (ki != Sqlx.UNION && dk == Sqlx.UNION)
                for (var b = dt.unionOf.First(); b != null; b = b.Next())
                    if (b.key() is Domain dm && dm.Constrain(cx,lp,this).kind != Sqlx.Null)
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
                    return Null;
                if (nt.Count == 1)
                    return nt.First()?.key()??Null;
                return (Domain)cx.Add(new Domain(cx.GetUid(), Sqlx.UNION, nt));
            }
            else if (elType is not null && ce is not null)
                r = (Domain)cx.Add(new Domain(cx.GetUid(),kind, elType.LimitBy(cx,lp, ce)));
            else if (ki == Sqlx.ROW && dt == TableType)
                return this;
            else if ((ki == Sqlx.ROW || ki == Sqlx.TYPE) && (dk == Sqlx.ROW || dk == Sqlx.TABLE))
                return dt;
            else if ((ki != Sqlx.ROW && ki != dk) || (ki == Sqlx.ROW && dk != Sqlx.ROW) ||
                    (elType is null) != (ce == null) || orderFunc != dt.orderFunc || orderflags != dt.orderflags)
                return Null;
            if ((dt.prec != 0 && prec != 0 && prec != dt.prec) || (dt.scale != 0 && scale != 0 && scale != dt.scale) ||
                (dt.name != "" && name != "" && name != dt.name) || start != dt.start || end != dt.end ||
                (dt.charSet != CharSet.UCS && charSet != CharSet.UCS && charSet != dt.charSet) ||
                (dt.culture != CultureInfo.InvariantCulture && culture != CultureInfo.InvariantCulture && culture != dt.culture))
                //             (dt.defaultValue != "" && defaultValue != "" && defaultValue != dt.defaultValue)
                return Null;
            if ((prec != dt.prec || scale != dt.scale || name != dt.name ||
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
                if (dt.defaultValue!=TNull.Value && dt.defaultValue != r.defaultValue)
                    m += (Default, dt.defaultValue);
                else if (defaultValue!=TNull.Value && defaultValue != r.defaultValue)
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
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new Domain(dp,m);
        }
        internal override void _Add(Context cx)
        {
            base._Add(cx);
        }
        internal override DBObject Relocate(long dp, Context cx)
        {
            var r = (Domain)New(dp, _Fix(cx,mem));
            var ts = cx.newTypes;
            if (ts[r] is long p)
            {
                if (p != r.defpos)
                    return (Domain?)(cx.obs[p] ?? cx.db.objects[p]) ?? throw new PEException("PE1553");
                else
                    cx.newTypes = ts + (r, r.defpos);
            }
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = New(cx.Fix(defpos), _Fix(cx, mem));
            if (defpos != -1L)
                cx.Add(r);
            return r;
        }
        protected override BTree<long,object> _Fix(Context cx,BTree<long,object>m)
        {
            if (kind == Sqlx.Null)
                return m;
            var r = base._Fix(cx, m);
            if (constraints.Count > 0)
            {
                var nc = cx.FixTlb(constraints);
                if (constraints != nc)
                    r += (Constraints, nc);
            }
            var no = orderFunc?.Fix(cx);
            if (orderFunc != no && no is not null)
                r += (OrderFunc, no);
            if (representation.Count > 0)
            {
                var nr = cx.FixTlD(representation);
                if (representation != nr)
                    r += (Representation, nr);
            }
            if (elType is not null)
            {
                var et = elType.Fix(cx);
                if (et != elType)
                    r += (Element, et);
            }
            if (rowType.Count > 0)
            {
                var nt = cx.FixLl(rowType);
                if (rowType != nt)
                    r += (RowType, nt);
            }
            if (aggs.Count > 0)
            {
                var na = cx.FixTlb(aggs);
                if (aggs != na)
                    r += (Aggs, na);
            }
            if (mem.Contains(Default) && defaultValue!=TNull.Value)
                r += (Default, defaultValue.Fix(cx));
            return r;
        }
        protected override DBObject _Replace(Context cx, DBObject was, DBObject now)
        {
            var r = (Domain)base._Replace(cx, was, now);
            var ch = false;
            var cs = CTree<long, bool>.Empty;
            for (var b = constraints?.First(); b != null; b = b.Next())
            {
                var ck = (Check)cx._Replace(b.key(),was, now);
                ch = ch || b.key() != ck.defpos;
                cs += (ck.defpos, true);
            }
            if (ch)
                r += (Constraints, cs);
            if (elType is not null)
            {
                var e = elType.Replace(cx, was, now)??throw new PEException("PE1551");
                if (e != elType)
                    r += (Element, e);
            }
            var orf = orderFunc?.Replace(cx, was, now);
            if (orf != orderFunc && orf is not null)
                r +=(cx, OrderFunc, orf.defpos);
            var rs = CTree<long,Domain>.Empty;
            ch = false;
            for (var b = representation.First(); b != null; b = b.Next())
            {
                var od = b.value();
                var k = b.key();
                var dr = od?.Replace(cx, was, now)??throw new PEException("PE1550");
                if (k == was.defpos || k==now.defpos)
                {
                    k = now.defpos;
                    dr = (cx.done[now.defpos]??now); 
                }
                if (dr != od || k != b.key())
                    ch = true;
                if (dr is not null)
                    rs += (k, dr as Domain??dr.domain);
            }
            if (ch)
                r +=(cx, Representation, rs);
            var rt = BList<long?>.Empty;
            ch = false;
            for (var b=rowType.First();b is not null;b=b.Next())
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
                r +=(cx, RowType, rt);
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
                var nk = cx.done[b.key()]?.defpos??cx.uids[b.key()]??b.key();
                var rr = od.Replaced(cx);
                if (rr != od || nk != b.key())
                    ch = true;
                rs += (nk, rr);
            }
            if (ch)
                r += (Representation, rs);
            var rt = BList<long?>.Empty;
            ch = false;
            for (var b = rowType.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    var np = cx.done[p]?.defpos ?? cx.uids[p]?? p;
                    if (p != np)
                        ch = true;
                    rt += np;
                }
            if (ch)
                r += (RowType, rt);
            return r;
        }
        internal override Basis ShallowReplace(Context cx, long was, long now)
        {
            if (defpos < -1L || kind==Sqlx.Null)
                return this;
            var r = this;
            var ag = cx.ShallowReplace(aggs, was, now);
            if (ag != aggs)
                r += (Aggs, ag);
            var dv = defaultValue.ShallowReplace(cx, was, now);
            if (dv != defaultValue)
                r += (Default, dv);
            var rv = cx.ShallowReplace(defaultRowValues, was, now);
            if (rv != defaultRowValues)
                r += (DefaultRowValues, rv);
            if (orderFunc != null && cx.db.objects[orderFunc.defpos] is Procedure pr && pr!=orderFunc)
                r += (OrderFunc, pr);
            var rs = cx.ShallowReplace(representation, was, now);
            if (rs != representation)
                r += (Representation, rs);
            var rt = cx.ShallowReplace(rowType, was, now);
            if (rt != rowType)
                r += (RowType, rt);
            return r;
        }
        internal override (BList<DBObject>, BTree<long, object>) Resolve(Context cx, long f, BTree<long, object> m)
        {
            var cs = BList<long?>.Empty;
            var rs = CTree<long, Domain>.Empty;
            var ch = false;
            var de = depth;
            BList<DBObject> ls;
            for (var b = First(); b != null; b = b.Next())
                if (b.value() is long p && cx.obs[p] is SqlValue v)
                {
                    (ls, m) = v.Resolve(cx, f, m);
                    if (ls[0] is SqlValue nv)
                    {
                        if (nv.defpos != v.defpos)
                        {
                            ch = true; v = nv;
                            de = Math.Max(de, Math.Max(nv.depth, nv.depth) + 1);
                        }
                        cs += v.defpos;
                        rs += (v.defpos, v.domain);
                    }
                }
            if (ch)
            {
                var r = this;
                if (r.defpos < Transaction.Analysing)
                    r = (Domain)Relocate(cx.GetUid());
                r = (Domain)cx.Add((Domain)r.New(r.mem + (RowType, cs) + (Representation, rs)));
                return (new BList<DBObject>(r), m);
            }
            return (new BList<DBObject>(this), m);
        }
        public static string NameFor(Context cx, long p, int i)
        {
            var sv = cx._Ob(p);
            return sv?.infos[cx.role.defpos]?.name
                ?? sv?.alias ?? (string?)sv?.mem[ObInfo.Name] ?? ("Col" + i);
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
            var sb = new StringBuilder();
            sb.Append("<PyrrhoDBType kind=\"" + kind + "\" ");
            if (name != "")
                sb.Append(",name=\"" + name + "\" ");
            bool empty = true;
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
            if (defaultValue != TNull.Value)
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
            StringBuilder sb = new ();
            switch (kind)
            {
                default:
                    //          sb.Append("type=\"" + ob.DataType.ToString() + "\">");
                    sb.Append(ob.ToString());
                    break;
                case Sqlx.ARRAY:
                    {
                        var a = (TList)ob;
                        //           sb.Append("type=\"array\">");
                        if (elType is not null && (cx.db.Find(elType)?.defpos ?? cx.newTypes[elType]) is long ep)
                            for (int j = 0; j < a.Length; j++)
                                sb.Append("<item " + elType.Xml(cx, ep, a[j]) + "</item>");
                        break;
                    }
                case Sqlx.MULTISET:
                    {
                        var m = (TMultiset)ob;
                        //          sb.Append("type=\"multiset\">");
                        if (elType is not null && (cx.db.Find(elType)?.defpos ?? cx.newTypes[elType]) is long ep)
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
                        if (ro != null)
                            sb.Append("<" + ro.name);
                        var ss = new string[r.Length];
                        var empty = true;
                        var i = 0;
                        for (var b = r.dataType.representation.First(); b != null; b = b.Next(), i++)
                            if (r[i] is TypedValue tv && b.value() is DBObject mo &&
                                mo.infos[cx.role.defpos] is ObInfo oi)
                            {
                                var kn = b.key();
                                var p = tv.dataType;
                                var m = oi.metadata;
                                if (tv != TNull.Value && m != null && m?.Contains(Sqlx.ATTRIBUTE) == true)
                                    sb.Append(" " + kn + "=\"" + tv.ToString() + "\"");
                                else if (tv != TNull.Value)
                                {
                                    ss[i] = "<" + kn + " type=\"" + p.ToString() + "\">" +
                                        p.Xml(cx, defpos, tv) + "</" + kn + ">";
                                    empty = false;
                                }
                            }
                        if (ro != null)
                        {
                            if (empty)
                                sb.Append('/');
                            sb.Append('>');
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
    }
    
    internal class StandardDataType : Domain
    {
        public static BTree<Sqlx, StandardDataType> types = BTree<Sqlx, StandardDataType>.Empty;
        internal StandardDataType(Sqlx t, OrderCategory oc = OrderCategory.None, 
            Domain? o = null, BTree<long, object>? u = null)
            : base(-(long)t, t, _Mem(oc, o, u))
        {
            types += (t, this);
            Context._system.db = Database._system;
        }
        protected StandardDataType(long dp, BTree<long, object> m) : base(dp, m) { }
        public static StandardDataType operator+(StandardDataType dt,(long,object) x)
        {
            var (dp, ob) = x;
            if (dt.mem[dp] == ob)
                return dt;
            return (StandardDataType)dt.New(dt.mem + x);
        }
        internal static StandardDataType Get(Sqlx undertok)
        {
            if (types[undertok] is StandardDataType st)
                return st;
            return (StandardDataType)Content;
        }
        static BTree<long,object> _Mem(OrderCategory oc, Domain? o, BTree<long, object>? u)
        {
            u ??= BTree<long, object>.Empty;
            if (o is not null)
                u += (Element, o);
            if (!u.Contains(Descending))
                u += (Descending, Sqlx.ASC);
            if (!u.Contains(_OrderCategory))
                u += (_OrderCategory, oc);
            return u;
        }
    }
    /// <summary>
    /// Security labels.
    /// Access rules: clearance C can access classification z if C.maxlevel>=z and 
    ///  can update if classification matches C.minlevel.
    /// Clearance allows minlevel LEQ laxlevel.
    /// Show classification minlevel==maxlevel always.
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
        public static readonly Level D = new ();
        Level() { }
        public Level(byte min, byte max, BTree<string, bool> g, BTree<string, bool> r)
        {
            minLevel = min; maxLevel = max; groups = g; references = r;
        }
        internal static void SerialiseLevel(Writer wr, Level lev)
        {
            if (wr.cx.db.levels.Contains(lev))
                wr.PutLong(wr.cx.db.levels[lev]??-1L);
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
                    return rdr.context.db.cache[lp]??D;
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
        public override bool Equals(object? obj)
        {
            if (obj is Level that)
            {
                if (minLevel != that.minLevel || maxLevel != that.maxLevel
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
            return false;
        }
        public override int GetHashCode()
        {
            return (int)(minLevel + maxLevel + groups.Count + references.Count);
        }
        static char For(byte b)
        {
            return (char)('D' - b);
        }
        static void Append(StringBuilder sb, BTree<string, bool> t, char s, char e)
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
        internal static Level Parse(string s,Context cx)
        {
            var sc = new Scanner(-1, s.ToCharArray(), 0, cx);
            byte low=0, high;
            var gps = BTree<string, bool>.Empty;
            var rfs = BTree<string, bool>.Empty;
            var ch = sc.Advance();
            if (ch >= 'A' && ch <= 'D')
                low = (byte)('D' - ch);
            sc.Advance();
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
        public int CompareTo(object? obj)
        {
            if (obj == null)
                return 1;
            Level that = (Level)obj;
            int c = minLevel.CompareTo(that.minLevel);
            if (c != 0)
                return c;
            c = maxLevel.CompareTo(that.maxLevel);
            if (c != 0)
                return c;
            var b = groups.First();
            var tb = that.groups.First();
            for (;c==0 && b is not null && tb is not null;b=b.Next(),tb=tb.Next())
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
        internal Context? _cx = null;
        /// <summary>
        /// Constructor: prepare the scanner
        /// Invariant: ch==input[pos]
        /// </summary>
        /// <param name="s">the input array</param>
        /// <param name="p">the starting position</param>
        internal Scanner(long t,char[] s, int p, Context cx, string m="text/plain")
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
            if (char.IsLetter(ch))
            {
                while (char.IsLetterOrDigit(ch))
                    Advance();
            }
            return new string(input, st, pos - st);
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

    internal class UDType : Table
    {
        internal const long
            Prefix = -390, // string
            Suffix = -400; // string
        public string? prefix => (string?)mem[Prefix];
        public string? suffix => (string?)mem[Suffix];
        internal override CTree<long, Domain> tableCols => pathDomain.representation;  
        public CTree<long, string> methods =>
            (CTree<long, string>?)mem[Database.Procedures] ?? CTree<long, string>.Empty;
        internal UDType(Sqlx t) : base(t) { }
        public UDType(long dp, BTree<long, object> m) : base(dp,m)
        { }
        public static UDType operator +(UDType et, (long, object) x)
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
            return (UDType)et.New(m + x);
        }
        public static UDType operator +(UDType et, (Context, long, object) x)
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
            return (UDType)et.New(m + (dp, ob));
        }
        public static UDType operator -(UDType t, long x)
        {
            return (UDType)t.New(t.mem - x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new UDType(defpos, m);
        }
        internal override DBObject New(long dp, BTree<long, object> m)
        {
            return new UDType(dp, m + (Kind, Sqlx.TYPE));
        }
        internal virtual UDType? New(Ident pn, Domain? un, long dp, Context cx)
        {
            return (UDType?)cx.Add(new PType(pn.ident, (UDType)TypeSpec.Relocate(dp), un, -1L, dp, cx));
        }
        internal Ident.Idents Defs(Context cx)
        {
            var lp = cx.GetUid();
            var ids = Ident.Idents.Empty;
            var ix = cx.Ix(defpos);
            for (var b = representation.First(); b != null; b = b.Next())
            {
                var dm = b.value();
                var cds = Ident.Idents.Empty;
                if (dm is UDType u)
                    cds = u.Defs(cx);
                var p = b.key();
                var c = (TableColumn?)cx.db.objects[p]??throw new PEException("PE1559");
                cx.Add(c);
                var px = cx.Ix(p);
                var cn = c.NameFor(cx);
                cx.defs += (cn, px, cds);
                ids += (cn, px, cds);
            }
            cx.defs += (name, ix, ids);
            for (var b = methods.First();b is not null;b=b.Next())
            {
                var mp = b.key();
                var me = (Method?)cx.db.objects[mp]??throw new PEException("PE1560");
                me.Instance(lp, cx, null);
            }
            return ids;
        }
        internal override DBObject Add(Context cx, PMetadata pm, long p)
        {
            var r = base.Add(cx,pm,p);
            if (pm.detail[Sqlx.PREFIX] is TChar pf)
            {
                r += (Prefix, pf.value);
                cx.db += (Database.Prefixes, cx.db.prefixes + (pf.value, defpos));
            }
            if (pm.detail[Sqlx.SUFFIX] is TChar sf)
            {
                r += (Suffix, sf.value);
                cx.db += (Database.Suffixes, cx.db.suffixes + (sf.value, defpos));
            }
            if (r != this)
            {
                cx.Add(r);
                cx.db += (defpos, r);
            }
            return r;
        }
        public override bool EqualOrStrongSubtypeOf(Domain dt)
        {
            for (Domain? s = this; s != null; s = (s as UDType)?.super)
                if (s.name==dt.name)
                    return true;
            return false;
        }
        internal override Table _PathDomain(Context cx)
        {
            var pd = (super as Table)?._PathDomain(cx);
            var rt = pd?.rowType ??BList<long?>.Empty;
            var rs = pd?.representation??CTree<long,Domain>.Empty;
            var ii = infos;
            if (pd is not null)
                for (var b = infos.First(); b != null; b = b.Next())
                    if (b.value() is ObInfo ti)
                    {
                        if (pd.infos[cx.role.defpos] is ObInfo si)
                            ti += si;
                        else throw new DBException("42105");
                        ii += (b.key(), ti);
                    }
            for (var b = pd?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && pd?.representation[p] is Domain cd && !rs.Contains(p))
                {
                    rt += p;
                    rs += (p, cd);
                }
            for (var b = rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && representation[p] is Domain cd && !rs.Contains(p))
                {
                    rt += p;
                    rs += (p, cd);
                }
            return new Table(cx,rs,rt,ii);
        }
        internal virtual UDType Inherit(UDType to)
        {
            return to;
        }
        bool IsSubTypeOf(UDType u)
        {
            for (UDType? s=this; s!=null; s= s.super as UDType)
                if (s.dbg==u.dbg)
                    return true;
            return false;
        }
        internal UDType? LowestCommonSuper(UDType a)
        {
            for (UDType? s = this; s != null; s = s.super as UDType)
                if (a.IsSubTypeOf(s))
                    return s;
            return null;
        }
        /// <summary>
        ///  We go to a lot of trouble to ensure that columns all have different names 
        ///  in any UDType subType/superType hierarchy. We use MergeColumn
        ///  whenever necessary to ensure this.
        /// </summary>
        /// <param name="cx"></param>
        /// <returns>a tree of all columns in subtypes with their defining type</returns>
        internal BTree<string,(int,long?)> AllSubTypeCols(Context cx)
        {
            var oi = infos[cx.role.defpos] ?? throw new DBException("42105");
            var r = BTree<string, (int, long?)>.Empty;
            for (var b = subtypes.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key()] is UDType u)
                    r += u.AllSubTypeCols(cx);
            return r + oi.names; // top levels overwrite lower
        }
        internal BTree<string,(int,long?)> HierarchyCols(Context cx)
        {
            for (var b = this;;)
                if (b.super is UDType nb)
                    b = nb;
                else
                    return b.AllSubTypeCols(cx);
            // not reached
        }
        public override bool HasValue(Context cx, TypedValue v)
        {
            var ki = Equivalent(kind);
            if (ki == Sqlx.UNION)
            {
                for (var d = v.dataType; d != null; d = (d as UDType)?.super)
                    if (mem.Contains(cx.db.Find(d)?.defpos ?? throw new PEException("PE48813")))
                        return true;
                return false;
            }
            return base.HasValue(cx, v);
        }

        protected override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = (UDType)base._Replace(cx, so,sv);
            if (super?.Replace(cx, so, sv) is Domain und
    && und != super)
                r += (Under, und);
            return r;
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var ns = super?.Fix(cx);
            if (super != ns && ns is not null)
                r += (Under, ns);
            var nu = cx.FixTlb(subtypes);
            if (nu != subtypes)
                r = cx.Add(r, Subtypes, nu);
            if (defaultString == "")
                r -= Default;
            return r;
        }
        internal override Domain Constrain(Context cx, long lp, Domain dt)
        {
             throw new DBException("42000",name);
        }
        /// <summary>
        /// Generate a row for the Role$Class table: includes a C# class definition
        /// </summary>
        /// <param name="cx"></param>
        /// <param name="from"></param>
        /// <param name="_enu"></param>
        /// <returns></returns>
        internal override TRow RoleClassValue(Context cx, RowSet from, ABookmark<long, object> _enu)
        {
            var ro = cx.role;
            var md = infos[ro.defpos]??throw new DBException("42105");
            var sb = new StringBuilder("using System;\r\nusing Pyrrho;\r\n");
            sb.Append("\r\n/// <summary>\r\n");
            sb.Append("/// Class " + md.name + " from Database " + cx.db.name
                + ", Role " + ro.name + "\r\n");
            if (md.description != "")
                sb.Append("/// " + md.description + "\r\n");
            sb.Append("/// </summary>\r\n");
            sb.Append("public class " + md.name + ((super is not null)?" : "+super.name:"") + " {\r\n");
            for (var b = representation.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var co = (DBObject?)cx.db.objects[p] ?? throw new DBException("42105");
                var dt = b.value();
                var tn = ((dt.kind == Sqlx.TYPE) ? dt.name : dt.SystemType.Name) + "?";
                dt.FieldType(cx, sb);
                if (co.infos[cx.role.defpos] is ObInfo ci &&  ci.description?.Length > 1)
                    sb.Append("  // " + ci.description + "\r\n");
                sb.Append("  public " + tn + " " + co.NameFor(cx) + ";");
                sb.Append("\r\n");
            }
            sb.Append("}\r\n");
            return new TRow(from, new TChar(md.name??"??"), new TChar(""),
                new TChar(sb.ToString()));
        }
        internal override int Typecode()
        {
            if (prefix != null || suffix != null)
                return 3; // string
            if (super != null)
                return super.Typecode();
            return 12;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(Uid(defpos));
            sb.Append(' '); sb.Append(base.ToString());
            if (mem.Contains(Under))
            {
                sb.Append(" Under="); sb.Append(super);
                sb.Append(" PathDomain="); sb.Append(pathDomain);
            }
            var cm = " Methods: ";
            for (var b=methods.First();b is not null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(Uid(b.key()));
                sb.Append(' ');
                sb.Append(b.value());
            }
            if (subtypes!=CTree<long,bool>.Empty)
            {
                sb.Append(" Subtypes [");cm = "";
                for (var b=subtypes.First();b is not null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key()));
                }
                sb.Append(']');
            }
            return sb.ToString();
        }
    }

}
