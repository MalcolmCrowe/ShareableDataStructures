using System;
using System.Globalization;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
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
    /// Immutable (everything in level 3 must be immutable)
    /// </summary>
    internal class Domain : DBObject, IComparable
    {
        // Annoyingly, I can't seem to make these definitions immutable
        internal static Domain Null, Value, Content, // Pyrrho 5.1 default type for Document entries, from 6.2 for generic scalar value
    Bool,Blob,Char,Password,XML,Int,Numeric,Real,Date,Timespan,Timestamp,
    Interval,TypeSpec,_Level,Physical,MTree, // pseudo type for MTree implementation
    Partial, // pseudo type for MTree implementation
    Array,Multiset,Collection,Cursor,UnionNumeric,UnionDate,
    UnionDateNumeric,Exception,Period,
    Document,DocArray,ObjectId,JavaScript,ArgList, // Pyrrho 5.1
    TableType,Row,Delta,
    RdfString,RdfBool,RdfInteger,RdfInt,RdfLong,RdfShort,RdfByte,RdfUnsignedInt,
    RdfUnsignedLong,RdfUnsignedShort,RdfUnsignedByte,RdfNonPositiveInteger,
    RdfNonNegativeInteger,RdfPositiveInteger,RdfNegativeInteger,RdfDecimal,
    RdfDouble, RdfFloat,RdfDate,RdfDateTime;
        internal static Domain UnionType(params Domain[] ut)
        {
            var u = BTree<long, object>.Empty;
            foreach (var d in ut)
                u += (d.defpos, d);
            return new Domain(Sqlx.UNION, u);
        }
        // Uids used in Domains, Tables, TableColumns, Functions to define mem.
        // named mem will have positive uids.
        // union types add mem of form Domain_uid->true.
        // indexes are handled separately.
        // Usage: D=Domain,C=TableColumn,T=Table,F=Function,E=Expression
        internal const long
            Abbreviation = -73, // string (D)
            Charset = -74, // Pyrrho.Common.CharSet (C E)
            Columns = -75, // BList<Selector> (D)
            Constraints = -76,  // BTree<long,Check> 
            Culture = -77, // System.Globalization.CultureInfo (C E)
            Default = -78, // TypedValue (D C)
            Descending = -79, // Sqlx
            Element = -80, // Domain (D)
            End = -81, // Sqlx (interval part) (D)
            Iri = -82, // string (D C T)
            Kind = -83, // Sqlx
            Names = -84, // BTree<string,Selector> (D T V)
            NullsFirst = -85, // true (C)
            OrderCategory = -86, // OrderCategory
            OrderFunc = -87, // Procedure (D)
            Precision = -88, // int (D)
            Provenance = -89, // string (D)
            Representation = -90, // Domain (D)
            Scale = -91, // int (D)
            Search = -92, // SqlValue (D) added to Constraints
            Start = -93, // Sqlx (D)
            Under = -94; // Domain (D)
        internal static void StandardTypes()
        {
            Null = new StandardDataType(Sqlx.Null);
            Value = new StandardDataType(Sqlx.VALUE); // Not known whether scalar or row type
            Content = new StandardDataType(Sqlx.CONTENT); // Pyrrho 5.1 default type for Document entries, from 6.2 for generic scalar value
            Bool = new StandardDataType(Sqlx.BOOLEAN);
            Blob = new StandardDataType(Sqlx.BLOB);
            Char = new StandardDataType(Sqlx.CHAR);
            Password = new StandardDataType(Sqlx.PASSWORD);
            XML = new StandardDataType(Sqlx.XML);
            Int = new StandardDataType(Sqlx.INTEGER);
            Numeric = new StandardDataType(Sqlx.NUMERIC);
            Real = new StandardDataType(Sqlx.REAL);
            Date = new StandardDataType(Sqlx.DATE);
            Timespan = new StandardDataType(Sqlx.TIME);
            Timestamp = new StandardDataType(Sqlx.TIMESTAMP);
            Interval = new StandardDataType(Sqlx.INTERVAL);
            TypeSpec = new StandardDataType(Sqlx.TYPE);
            _Level = new StandardDataType(Sqlx.LEVEL);
            Physical = new StandardDataType(Sqlx.DATA);
            MTree = new StandardDataType(Sqlx.M); // pseudo type for MTree implementation
            Partial = new StandardDataType(Sqlx.T); // pseudo type for MTree implementation
            Array = new StandardDataType(Sqlx.ARRAY, Content);
            Multiset = new StandardDataType(Sqlx.MULTISET, Content);
            Collection = UnionType(Array, Multiset);
            Cursor = new StandardDataType(Sqlx.CURSOR);
            UnionNumeric = UnionType(Int, Numeric, Real);
            UnionDate = UnionType(Date, Timespan, Timestamp, Interval);
            UnionDateNumeric = UnionType(Date, Timespan, Timestamp, Interval, Int, Numeric, Real);
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
        }
        internal static void RdfTypes()
        {
            RdfString = new Domain(Char, IriRef.STRING);
            RdfBool = new Domain(Bool, IriRef.BOOL);
            RdfInteger = new Domain(Int, IriRef.INTEGER);
            RdfInt = new Domain(Int, IriRef.INT, "value>=-2147483648 and value<=2147483647");
            RdfLong = new Domain(Int, IriRef.LONG, "value>=-9223372036854775808 and value<=9223372036854775807");
            RdfShort = new Domain(Int, IriRef.SHORT, "value>=-32768 and value<=32768");
            RdfByte = new Domain(Int, IriRef.BYTE, "value>=-128 and value<=127");
            RdfUnsignedInt = new Domain(Int, IriRef.UNSIGNEDINT, "value>=0 and value<=4294967295");
            RdfUnsignedLong = new Domain(Int, IriRef.UNSIGNEDLONG, "value>=0 and value<=18446744073709551615");
            RdfUnsignedShort = new Domain(Int, IriRef.UNSIGNEDSHORT, "value>=0 and value<=65535");
            RdfUnsignedByte = new Domain(Int, IriRef.UNSIGNEDBYTE, "value>=0 and value<=255");
            RdfNonPositiveInteger = new Domain(Int, IriRef.NONPOSITIVEINTEGER, "value<=0");
            RdfNonNegativeInteger = new Domain(Int, IriRef.NEGATIVEINTEGER, "value<0");
            RdfPositiveInteger = new Domain(Int, IriRef.POSITIVEINTEGER, "value>0");
            RdfNegativeInteger = new Domain(Int, IriRef.NONNEGATIVEINTEGER, "value>=0");
            RdfDecimal = new Domain(Numeric, IriRef.DECIMAL);
            RdfDouble = new Domain(Real, IriRef.DOUBLE);
            RdfFloat = new Domain(new Domain(Sqlx.REAL, BTree<long, object>.Empty + (Precision, 6)),
                IriRef.FLOAT);
            RdfDate = new Domain(Date, IriRef.DATE);
            RdfDateTime = new Domain(Timestamp, IriRef.DATETIME);
        }
        public Sqlx kind => (Sqlx)(mem[Kind]??Sqlx.NO);
        public BList<Selector> columns =>
            (BList<Selector>)mem[Columns] ?? BList<Selector>.Empty;
        public Domain super => (Domain)mem[Under];
        public string iri => (string)mem[Iri];
        public int prec => (int)(mem[Precision]??0);
        public int scale => (int)(mem[Scale]??0);
        public Sqlx start => (Sqlx)(mem[Start] ?? Sqlx.NULL);
        public Sqlx end => (Sqlx)(mem[End] ?? Sqlx.NULL);
        public Sqlx AscDesc => (Sqlx)(mem[Descending] ?? Sqlx.ASC);
        public Sqlx Nulls => ((bool)(mem[NullsFirst] ?? false)) ? Sqlx.FIRST : Sqlx.NULL;
        public CharSet charSet => (CharSet)(mem[Charset] ?? CharSet.UCS);
        public CultureInfo culture => (CultureInfo)(mem[Culture] ?? CultureInfo.InvariantCulture);
        public Domain elType => (Domain)(mem[Element] ?? StandardDataType.Null);
        public TypedValue defaultValue => Parse((string)mem[Default]);
        public string abbrev => (string)mem[Abbreviation]??"";
        public BTree<long, Check> constraints => (BTree<long, Check>)mem[Constraints];
        public string provenance => (string)mem[Provenance];
        public Domain representation => (Domain)mem[Representation];
        public Procedure orderFunc => (Procedure)mem[OrderFunc];
        public OrderCategory orderflags => (OrderCategory)(mem[OrderCategory]??Common.OrderCategory.None);
        public BTree<string, Selector> names => 
            (BTree<string, Selector>)mem[Names]??BTree<string,Selector>.Empty;
        public SqlValue search => constraints[Search].search;
        public int Length => (int)columns.Count;
        /// <summary>
        /// The first three constructors are used by subclasses
        /// </summary>
        /// <param name="t">The type e.g. Sqlx.TABLE</param>
        /// <param name="dp">The defining position</param>
        /// <param name="dr">The definer</param>
        /// <param name="u">Other properties</param>
        protected Domain(Sqlx t,long dp, BTree<long, object> u) 
            : base(dp, u+(Kind,t))
        {
 //           if (dp == -1)
 //               Database._system += (Database._system.schemaRole, this);
        }
        protected Domain(Sqlx t, long pp, long dp, long dr, BTree<long, object> u) 
            : base(pp, dp, dr, u + (Kind,t))
        {
        }
        protected Domain(Sqlx t, string nm, long pp, long dp, long dr, BTree<long, object> u)
            : base(nm, pp, dp, dr, u + (Kind,t))
        {
        }
        protected Domain(Domain t,string iri,string search)
            :this(Sqlx.RDFTYPE,iri,-1,--_uid,Database.Schema,t.mem+(Iri,iri)
                 +(Constraints,new BTree<long,object>(Search,
                     new Parser(Database._system).ParseSqlValue(search,Bool))))
        { }
        protected Domain(Domain t, string iri)
            : this(t.kind, iri, -1, --_uid, Database.Schema, t.mem + (Iri, iri))
        { }
        // A simple standard type, or a union of standard types
        public Domain(Sqlx t, BTree<long, object> u)
            : base(-1, --_uid, -1, u + (Kind,t))
        {
            Database._system += (Database._system.schemaRole, this);
        }
        // unusually in this framework, it is okay to have many Domains with the default -1 defpos
        protected Domain(long dp,BTree<long, object> m) : base(dp,m)
        { }
        /// <summary>
        /// Allow construction of ad-hoc Row types
        /// </summary>
        /// <param name="colums"></param>
        public Domain(BList<Selector> columns)
            : this(Sqlx.ROW, new BTree<long, object>(Columns, columns)
                  +(Names,_Names(columns))) { }
        /// <summary>
        /// Allow construction of ad-hoc derived types such as ARRAY, MULTISET
        /// </summary>
        /// <param name="t"></param>
        /// <param name="d"></param>
        public Domain(Sqlx t, Domain d)
            : this(t, new BTree<long, object>(Element, d))
        { }
        public Domain(BList<SqlValue> cols)
            : this(_Selectors(cols))
        { }
        /// <summary>
        /// Constructor: a newly defined Domain
        /// </summary>
        /// <param name="p">The PDomain level 2 definition</param>
        public Domain(PDomain p,BTree<long,object> m=null) : base(p.ppos, p.ppos, p.role.defpos,
            m??new BTree<long,object>(Kind,p.kind))
        { }
        static BTree<string,Selector> _Names(BList<Selector> cols)
        {
            var r = BTree<string, Selector>.Empty;
            for (var b = cols.First(); b != null; b = b.Next())
            {
                var s = b.value();
                r += (s.name, s);
            }
            return r;
        }
        static BList<Selector> _Selectors(BList<SqlValue> cols)
        {
            var r = BList<Selector>.Empty;
            var i = 0;
            for (var b = cols.First(); b != null; b = b.Next())
            {
                if (b.value() is Selector s)
                {
                    s += (Selector.Seq, i++);
                    if (s.alias != null)
                        s += (Name, s.alias);
                    r += s;
                }
                else
                {
                    var x = b.value();
                    r += new Selector(x.alias ?? x.name, x.defpos, 
                        x.nominalDataType, i++);
                }
            }
            return r;
        }
        public static Domain operator +(Domain d, (long, object) e)
        {
            return new Domain(d.defpos, d.mem + e);
        }
        public static Domain operator+(Domain d,Selector s)
        {
            d += (Columns, d.columns + (s.seq, s));
            return d+ (Names, d.names +(s.name, s));
        }
        public static Domain operator+ (Domain d,Metadata md)
        {
            var m = d.mem;
            if (md.description != "") m += (Description, md.description);
            if (md.iri != "") m += (Iri, md.iri);
            return (Domain)d.New(m);
        }
        public static Domain operator+(Domain d,(Sqlx,Sqlx)o)
        {
            if (d.AscDesc != o.Item1)
                d += (Descending, o.Item1);
            if (d.Nulls != o.Item2)
                d += (NullsFirst, o.Item2);
            return d;
        }
        internal override DBObject Add(Check ck, Database db)
        {
            return new Domain(kind,defpos,mem+(Constraints,constraints+(ck.defpos,ck)));
        }
        internal override DBObject AddProperty(Check ck, Database db)
        {
            return new Domain(kind,defpos,mem+(Properties,properties+(ck.name,ck.defpos)));
        }
        internal Domain For(Transaction tr,DBObject ob,Grant.Privilege pr)
        {
            if (ob.Denied(tr, pr))
                return Null;
            var cs = columns;
            for (var b = cs.First(); b != null; b = b.Next())
                if (!b.value().Denied(tr, pr))
                    cs += (b.key(), b.value());
            return (cs.Count == columns.Count)? this: 
                (cs==BTree<int,Selector>.Empty)? Null: this+(Columns,cs);
        }
        /// <summary>
        /// A readable version of the Domain
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder(" Domain: "+name);
            sb.Append(' ');sb.Append(kind);
            if (mem.Contains(Abbreviation)) { sb.Append(' '); sb.Append(abbrev); }
            if (mem.Contains(Columns) && Length!=0)
            { sb.Append(" Columns:"); var cm = "(";
                for (var b=columns.First();b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = ","; sb.Append(b.value().name);
                }
                sb.Append(")");
            }
            if (mem.Contains(Charset) && charSet != CharSet.UCS)
            { sb.Append(" CharSet="); sb.Append(charSet); }
            if (mem.Contains(Culture) && culture != CultureInfo.InvariantCulture)
            { sb.Append(" Culture="); sb.Append(culture.Name); }
            if (mem.Contains(Default) && defaultValue!=TNull.Value)
            { sb.Append(" Default="); sb.Append(defaultValue); }
            if (mem.Contains(Description)) { sb.Append(' '); sb.Append(desc); }
            if (mem.Contains(Element) && elType!=Null)
            { sb.Append(" elType="); sb.Append(elType); }
            if (mem.Contains(End)) { sb.Append(" End="); sb.Append(end); }
            // if (mem.Contains(Names)) { sb.Append(' '); sb.Append(names); } done in Columns
            if (mem.Contains(OrderCategory) && orderflags!=Common.OrderCategory.None)
            { sb.Append(' '); sb.Append(orderflags); }
            if (mem.Contains(OrderFunc)) { sb.Append(" OrderFunc="); sb.Append(orderFunc); }
            if (mem.Contains(Precision)) { sb.Append(" Prec="); sb.Append(prec); }
            if (mem.Contains(Provenance)) { sb.Append(" Provenance="); sb.Append(provenance); }
            if (mem.Contains(Representation)) { sb.Append(" Representation="); sb.Append(representation); }
            if (mem.Contains(Scale) && scale!=0) { sb.Append(" Scale="); sb.Append(scale); }
            if (mem.Contains(Sensitive)) sb.Append(" sensitive");
            if (mem.Contains(Start)) { sb.Append(" Start="); sb.Append(start); }
            if (mem.Contains(Under)) { sb.Append(" Under="); sb.Append(super); }
            if (mem.Contains(NullsFirst) && Nulls != Sqlx.NO) sb.Append(" Nulls First");
            return sb.ToString();
        }

        internal class IriRef
        {
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
        /// <summary>
        /// Creator: ensure the given anonymous Domain is defined in a given database
        /// </summary>
        /// <param name="db">The database</param>
        /// <param name="tp">The level 4 type</param>
        /// <returns>The new Domain (or null for a structured type)</returns>
        internal static (Database, Domain) Create(Database db, Domain tp, long u)
        {
            if (db.types.Contains(tp))
                tp = db.types[tp];
            else if (db is Transaction tr)
            {
                var pd = new PDomain(tp.name, tp, u, tr);
                tr += pd;
                return (tr, tr.GetDomain(u));
            }
            return (db, tp);
        }

        internal static Sqlx Equivalent(Sqlx kind)
        {
            switch (kind)
            {
                case Sqlx.NCHAR:
                case Sqlx.CLOB:
                case Sqlx.NCLOB:
                case Sqlx.VARCHAR: return Sqlx.CHAR;
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
        internal int Typecode()
        {
            switch (Equivalent(kind))
            {
                case Sqlx.NULL: return 0;
                case Sqlx.INTEGER: return 1;
                case Sqlx.NUMERIC: return 2;
                case Sqlx.REAL: return 8;
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
        public int? Get(ref Ident n,out Ident sub)
        {
            if (names[n.ident] is Selector s)
            {
                sub = n.sub;
                n = new Ident(n,s.segpos);
                return s.seq;
            }
            sub = null;
            return null;
        }
        public TypedValue Get(Reader rdr)
        {
            if (kind == Sqlx.SENSITIVE)
                return new TSensitive(this, elType.Get(rdr));
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
                            r.details.Add(new Delta.Action(ix, h, nm, Get(rdr)));
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
                        var el = rdr.db.GetDomain(dp);
                        var a = new TArray(el);
                        var n = rdr.GetInt();
                        for (int j = 0; j < n; j++)
                        {
                            var dt = el.GetDataType(rdr);
                            a[j] = dt.Get(rdr);
                        }
                        return a;
                    }
                case Sqlx.MULTISET:
                    {
                        var el = rdr.db.GetDomain(rdr.GetLong());
                        var m = new TMultiset(el);
                        var n = rdr.GetInt();
                        for (int j = 0; j < n; j++)
                        {
                            var dt = el.GetDataType(rdr);
                            m.Add(dt.Get(rdr));
                        }
                        return m;
                    }
                case Sqlx.REF:
                case Sqlx.ROW:
                    {
                        var dt = rdr.db.GetDomain(rdr.GetLong());
                        var r = new TypedValue[dt.columns.Count];
                        var n = rdr.GetInt();
                        for (var b = dt.columns.First(); b != null; b = b.Next())
                        {
                            var ct = b.value().domain;
                            var at = ct.GetDataType(rdr);
                            r[b.key()] = ct.Coerce(at.Get(rdr));
                        }
                        return new TRow(dt, r);
                    }
                case Sqlx.TYPE: goto case Sqlx.ROW;
            }
            throw new DBException("3D000", rdr.db.name).ISO();
        }
        public Domain GetDataType(Reader rdr)
        {
            var b = (DataType)rdr.ReadByte();
            if (b == DataType.Null)
                return null;
            if (b == DataType.DomainRef)
                return rdr.db.GetDomain(rdr.GetLong());
            return this;
        }
        /// <summary>
        /// Test for when to record subtype information. We want to do this when a value of
        /// a subtype is recorded in a column of the parent type, and the subtype information is
        /// not obtainable from the value alone. E.g. extra semantic information
        /// </summary>
        /// <param name="dt">The target type to check</param>
        /// <returns>true if this is a strong subtype of dt</returns>
        public bool EqualOrStrongSubtypeOf(Domain dt)
        {
            if (dt.defpos == defpos)
                return true;
            if (kind == Sqlx.SENSITIVE || dt.kind == Sqlx.SENSITIVE)
            {
                if (kind == Sqlx.SENSITIVE && dt.kind == Sqlx.SENSITIVE)
                    return elType.EqualOrStrongSubtypeOf(dt.elType);
                return false;
            }
            if (dt == null)
                return true;
            var ki = Equivalent(kind);
            var dk = Equivalent(dt.kind);
            if (dk == Sqlx.CONTENT || dk == Sqlx.Null)
                return true;
            if (ki == Sqlx.ONLY)
                return super.Equals(dt);
            if ((ki != Sqlx.ROW && ki != dk) || (ki == Sqlx.ROW && dk != Sqlx.ROW) ||
                (elType == null) != (dt.elType == null))
                return false;
            if (elType != null && !elType.EqualOrStrongSubtypeOf(dt.elType))
                return false;
            if (ki == Sqlx.UNION && dk == Sqlx.UNION)
            {
                for (var b = mem.First(); b != null; b = b.Next())
                    if (b.value() is Domain dm && !dt.mem.Contains(dm.defpos))
                        return false;
                return true;
            }
            if (dk == Sqlx.UNION)
                for (var b = dt.mem.First(); b != null; b = b.Next())
                    if (b.value() is Domain dm && EqualOrStrongSubtypeOf(dm))
                        return true;
            for (Domain s = this; s != null; s = s.super)
                if (s.Equals(dt))
                    return true;
            if (Length != dt.Length)
                return false;
            return (dt.prec == 0 || prec == dt.prec) && (dt.scale == 0 || scale == dt.scale) &&
                (dt.start == Sqlx.NULL || start == dt.start) &&
                (dt.end == Sqlx.NULL || end == dt.end) && (dt.charSet == CharSet.UCS || charSet == dt.charSet) &&
                (dt.culture == CultureInfo.InvariantCulture || culture == dt.culture);
        }

        /// <summary>
        /// Output the actual data type details if not the same as nominal data type
        /// </summary>
        /// <param name="wr"></param>
        public void PutDataType(Domain nt, WriterBase wr)
        {
            if (nt.kind == Sqlx.ONLY || defpos < 0)
            {
                var at = nt.super;
                nt.PutDataType(nt, wr);
                return;
            }
            if (EqualOrStrongSubtypeOf(nt) && !Equals(nt))
            {
                wr.WriteByte((byte)DataType.DomainRef);
                wr.PutLong(defpos);
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
                    case Sqlx.REF:
                    case Sqlx.ROW: wr.WriteByte((byte)DataType.Row); break;
                }
        }
        public void Put(TypedValue tv, Writer wr)
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
                        wr.PutInt(d.details.Count);
                        foreach (var de in d.details)
                        {
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
                            wr.PutBytes(n.ivalue.bytes);
                        break;
                    }
                case Sqlx.NUMERIC:
                    {
                        var d = tv.Val() as Numeric;
                        if (tv is TInt)
                            d = new Numeric(tv.ToLong().Value);
                        if (tv is TInteger)
                            d = new Numeric((Integer)tv.Val(), 0);
                        wr.PutBytes(d.mantissa.bytes);
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
                        wr.PutBytes(d.mantissa.bytes);
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
                        var rw = tv as TRow;
                        wr.PutLong(defpos);
                        var st = rw.dataType;
                        for (var b = st.columns.First(); b != null; b = b.Next())
                        {
                            var tc = b.value();
                            wr.PutLong(tc.domain.defpos);
                            tc.domain.Put(rw[b.key()], wr);
                        }
                        break;
                    }
                case Sqlx.TYPE: goto case Sqlx.ROW;
                case Sqlx.REF: goto case Sqlx.ROW;
                case Sqlx.ARRAY:
                    {
                        var a = (TArray)tv;
                        wr.PutLong(a.dataType.elType.defpos);
                        wr.PutInt(a.Length);
                        foreach (var e in a.list)
                            a.dataType.elType.Put(e, wr);
                        break;
                    }
                case Sqlx.MULTISET:
                    {
                        TMultiset m = (TMultiset)tv;
                        wr.PutLong(m.dataType.elType.defpos);
                        wr.PutInt((int)m.Count);
                        for (var a = m.tree.First(); a != null; a = a.Next())
                            if (a.value().HasValue)
                                for (int i = 0; i < a.value().Value; i++)
                                    m.dataType.elType.Put(a.key(), wr);
                        break;
                    }
            }
        }
        public int CompareTo(object obj)
        {
            var that = (Domain)obj;
            var c = kind.CompareTo(that.kind);
            if (c != 0)
                return c;
            var b = mem.First();
            var tb = that.mem.First();
            for (; c == 0 && b != null && tb != null; b = b.Next(), tb = tb.Next())
            {
                c = b.key().CompareTo(tb.key());
                if (c == 0 && b.key()!=Ppos && b.key()!=Definer)
                {
                    var v = b.value();
                    var tv = tb.value();
                    if (v is IComparable vc && tv is IComparable tc)
                        c = vc.CompareTo(tc);
                }
            }
            if (b != null)
                return 1;
            if (tb != null)
                return -1;
            return c;
        }
        /// <summary>
        /// Compare two values of this data type.
        /// (v5.1 allow the second to have type Document in all cases)
        /// </summary>
        /// <param name="a">the first value</param>
        /// <param name="b">the second value</param>
        /// <returns>-1,0,1 according as a LT,EQ,GT b</returns>
        public virtual int Compare(TypedValue a, TypedValue b)
        {
            var an = a == null || a == TNull.Value;
            var bn = b == null || b == TNull.Value;
            if (an && bn)
                return 0;
            if (an)
                return (Nulls == Sqlx.FIRST) ? 1 : -1;
            if (bn)
                return (Nulls == Sqlx.FIRST) ? -1 : 1;
            int c;
            if (kind == Sqlx.SENSITIVE)
            {
                a = (a is TSensitive sa) ? sa.value : a;
                b = (b is TSensitive sb) ? sb.value : b;
                c = elType.Compare(a, b);
                goto ret;
            }
            if (orderflags != Common.OrderCategory.None)
            {
                var n = orderFunc.name;
                var sa = new SqlLiteral(-1,a);
                var sb = new SqlLiteral(-1,b);
                var cx = new Context(Database._system);
                    if ((orderflags & Common.OrderCategory.Relative) == Common.OrderCategory.Relative)
                {

                    orderFunc.Exec(null, cx, new BList<SqlValue>(sa) + sb);
                    c = cx.ret.ToInt().Value;
                    goto ret;
                }
                orderFunc.Exec(null,cx,new BList<SqlValue>(sa));
                a = cx.ret;
                orderFunc.Exec(null,cx,new BList<SqlValue>(sb));
                b = cx.ret;
                c = a.dataType.Compare(a, b);
                goto ret;
            }
            switch (Equivalent(kind))
            {
                case Sqlx.BOOLEAN: return (a.ToBool().Value).CompareTo(b.ToBool().Value);
                case Sqlx.CHAR:
                    c = string.Compare(a.ToString(), b.ToString(), false, culture);
                    break;
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
                case Sqlx.NUMERIC: c = ((Numeric)a.Val()).CompareTo(b.Val()); break;
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
                        c = dcb.Query(b);
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
                        if (x == null || y == null)
                            throw new DBException("22004").ISO();
                        if (x.dataType.elType != y.dataType.elType)
                            throw new DBException("22202").Mix()
                                .AddType(x.dataType.elType).AddValue(y.dataType);
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
                            c = x.dataType.elType.Compare(x[j], y[j]);
                            if (c != 0)
                                break;
                        }
                        break;
                    }
                case Sqlx.MULTISET:
                    {
                        var x = a as TMultiset;
                        var y = b as TMultiset;
                        if (x == null || y == null)
                            throw new DBException("22004").ISO();
                        if (x.dataType.elType != y.dataType.elType)
                            throw new DBException("22202").Mix()
                                .AddType(x.dataType.elType).AddValue(y.dataType.elType);
                        var e = x.tree.First();
                        var f = y.tree.First();
                        for (; ; )
                        {
                            while (e != null && !e.value().HasValue)
                                e = e.Next();
                            while (f != null && !f.value().HasValue)
                                f = f.Next();
                            if (e == null || f == null)
                                break;
                            c = elType.Compare(e.key(), f.key());
                            if (c != 0)
                                goto ret;
                            c = e.value().Value.CompareTo(f.value().Value);
                            if (c != 0)
                                goto ret;
                            break;
                        }
                        while (e != null && !e.value().HasValue)
                            e = e.Next();
                        while (f != null && !f.value().HasValue)
                            f = f.Next();
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
                        int n = Length;
                        TRow ra = a as TRow;
                        TRow rb = b as TRow;
                        if (ra == null || rb == null)
                            throw new DBException("22004").ISO();
                        if (ra.Length != rb.Length)
                            throw new DBException("22202").Mix()
                                .AddType(ra.dataType).AddValue(rb.dataType);
                        c = a.dataType.Compare(a, b);
                        break;
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
                        for (var bb = mem.First(); bb != null; bb = bb.Next())
                            if (bb.value() is Domain dt)
                                if (dt.CanTakeValueOf(a.DataType) && dt.CanTakeValueOf(b.DataType))
                                    return dt.Compare(a, b);
                        throw new DBException("22202", a.dataType.ToString(), b.dataType.ToString());
                    }
                case Sqlx.PASSWORD:
                    throw new DBException("22202").ISO();
                default: c = a.ToString().CompareTo(b.ToString()); break;
            }
            ret:
            return (AscDesc==Sqlx.DESC)?-c:c;
        }
        /// <summary>
        /// Creator: Add the given array at the end of this
        /// </summary>
        /// <param name="a"></param>
        /// <returns></returns>
        public TArray Concatenate(TArray a, TArray b)
        {
            var r = new TArray(this);
            if (a.dataType.elType != elType || a.dataType.elType != b.dataType.elType)
                throw new DBException("22102").Mix()
                    .AddType(a.dataType.elType).AddValue(b.dataType.elType);
            int j = 0;
            foreach (var e in a.list)
                r[j++] = e;
            foreach (var e in b.list)
                r[j++] = e;
            return r;
        }
        /// <summary>
        /// Test a given type to see if its values can be assigned to this type
        /// </summary>
        /// <param name="dt">The other data type</param>
        /// <returns>whether values of the given type can be assigned to a variable of this type</returns>
        public virtual bool CanTakeValueOf(Domain dt)
        {
            if (ppos < 0) // a system domain currently being defined? Worry about values later 
                return true;
            if (dt.kind == Sqlx.SENSITIVE)
            {
                if (kind == Sqlx.SENSITIVE)
                    return elType.CanTakeValueOf(dt.elType);
                return false;
            }
            if (kind == Sqlx.SENSITIVE)
                return elType.CanTakeValueOf(dt);
            if (dt?.kind == Sqlx.ONLY)
                dt = dt.super;
            if (kind == Sqlx.VALUE || kind == Sqlx.CONTENT)
                return true;
            if (dt.kind == Sqlx.CONTENT || dt.kind == Sqlx.VALUE)
                return kind != Sqlx.REAL && kind != Sqlx.INTEGER && kind != Sqlx.NUMERIC;
            if (dt.kind == Sqlx.UNION)
            {
                for (var b = mem.First(); b != null; b = b.Next())
                    if (b.value() is Domain dm && CanTakeValueOf(dm))
                        return true;
                return false;
            }
            if (iri != null && iri != dt.iri)
                return false;
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
                case Sqlx.ROW:
                    {
                        int n = Length;
                        if (n != dt.Length)
                            return false;
                        if (columns == null || columns.Count != n)
                            throw new PEException("PE832");
                        if (dt.columns == null || dt.columns.Count != n)
                            return false;
                        for (int j = 0; j < n; j++) // for CanTakeValueOf, don't require names to match (INSERT)
                            if (!columns[j].domain.CanTakeValueOf(dt.columns[j].domain))
                                return false;
                        return true;
                    }
                case Sqlx.ARRAY:
                case Sqlx.MULTISET:
                    return kind == dt.kind && elType.CanTakeValueOf(dt.elType);
                case Sqlx.TYPE:
                    {
                        int n = Length;
                        if (n != dt.Length)
                            return false;
                        for (int j = 0; j < n; j++)
                            if (!columns[j].domain.CanTakeValueOf(dt.columns[j].domain))
                                return false;
                        return true;
                    }
#if SIMILAR
                case Sqlx.CHAR: return dk == Sqlx.REGULAR_EXPRESSION || dk == ki;
#endif
            }
        }

        public virtual bool HasValue(TypedValue v)
        {
            if (v is TSensitive st)
            {
                if (kind == Sqlx.SENSITIVE)
                    return elType.HasValue(st.value);
                return false;
            }
            if (kind == Sqlx.SENSITIVE)
                return elType.HasValue(v);
            var ki = Equivalent(kind);
            if (ki == Sqlx.ONLY || iri != null)
                return Equals(v.dataType); // must match exactly
            if (ki == Sqlx.NULL || kind == Sqlx.ANY)
                return true;
            if (ki == Sqlx.UNION)
                return mem.Contains(v.dataType.defpos);
            if (ki != v.dataType.kind)
                return false;
            switch (ki)
            {
                case Sqlx.MULTISET:
                case Sqlx.ARRAY:
                    return elType?.Equals(v.dataType.elType) ?? true;
                case Sqlx.TABLE:
                case Sqlx.TYPE:
                case Sqlx.ROW:
                    {
                        if (v.dataType.Length != Length)
                            return false;
                        for (int i = 0; i < v.dataType.Length; i++)
                            if (!v.dataType.columns[i].domain.EqualOrStrongSubtypeOf(columns[i].domain))
                                return false;
                        break;
                    }
            }
            return true;
        }
        public virtual TypedValue Parse(string s)
        {
            if (s == null)
                return TNull.Value;
            if (kind == Sqlx.SENSITIVE)
                return new TSensitive(this, elType.Parse(s));
            if (kind == Sqlx.DOCUMENT)
                return new TDocument(s);
            return Parse(new Scanner(s.ToCharArray(), 0));
        }
        public virtual TypedValue Parse(string s, string m)
        {
            if (kind == Sqlx.SENSITIVE)
                return new TSensitive(this, elType.Parse(s, m));
            if (kind == Sqlx.DOCUMENT)
                return new TDocument(s);
            return Parse(new Scanner(s.ToCharArray(), 0, m));
        }
        /// <summary>
        /// Parse a string value for this type. 
        /// </summary>
        /// <param name="lx">The scanner</param>
        /// <returns>a typedvalue</returns>
        public virtual TypedValue Parse(Scanner lx, bool union = false)
        {
            if (kind == Sqlx.SENSITIVE)
                return new TSensitive(this, elType.Parse(lx, union));
            int start = lx.pos;
            if (lx.Match("null"))
                return TNull.Value;
            switch (Equivalent(kind))
            {
                case Sqlx.Null:
                    {
                        int st = lx.pos;
                        int ln = lx.len - lx.pos;
                        var str = new string(lx.input, st, ln);
                        var lxr = new Lexer(str);
                        lx.pos += lxr.pos;
                        lx.ch = lxr.input[lxr.pos];
                        return lxr.val;
                    }
                case Sqlx.BOOLEAN:
                    if (lx.MatchNC("TRUE"))
                        return TBool.True;
                    if (lx.MatchNC("FALSE"))
                        return TBool.False;
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
                            return TNull.Value;
                        }
                        else
                        {
                            lx.pos = lx.len;
                            lx.ch = '\0';
                        }
                        if (prec != 0 && prec < str.Length)
                            str = str.Substring(0, prec);
                        if (charSet == CharSet.UCS || Check(str))
                            return new TChar(str);
                        break;
                    }
                case Sqlx.CONTENT:
                    {
                        var st = lx.pos;
                        var s = new string(lx.input, lx.pos, lx.input.Length - lx.pos);
                        var i = 0;
                        var c = TDocument.GetValue(null, s, s.Length, ref i);
                        lx.pos = lx.pos + i;
                        lx.ch = (lx.pos < lx.input.Length) ? lx.input[lx.pos] : '\0';
                        return c.typedValue;
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
                                        return new TNumeric(this, new Common.Numeric(x, 0));
                                    if (kind == Sqlx.REAL)
                                        return new TReal(this, (double)x);
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
                                    return new TInt(this, x);
                                }
                                else
                                {
                                    long x = long.Parse(str);
                                    if (kind == Sqlx.NUMERIC)
                                        return new TNumeric(this, new Common.Numeric(x));
                                    if (kind == Sqlx.REAL)
                                        return new TReal(this, (double)x);
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
                                    return new TInt(this, x);
                                }
                            }
                            if ((lx.ch != 'e' && lx.ch != 'E') || kind == Sqlx.NUMERIC)
                            {
                                str = lx.String(start, lx.pos - start);
                                Common.Numeric x = Common.Numeric.Parse(str);
                                if (kind == Sqlx.REAL)
                                    return new TReal(this, (double)x);
                                return new TNumeric(this, x);
                            }
                            lx.Advance();
                            if (lx.ch == '-' || lx.ch == '+')
                                lx.Advance();
                            if (!char.IsDigit(lx.ch))
                                throw new DBException("22107").Mix();
                            lx.Advance();
                            while (char.IsDigit(lx.ch))
                                lx.Advance();
                            str = lx.String(start, lx.pos - start);
                            return new TReal(this, (double)Common.Numeric.Parse(str));
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
                        return new TDateTime(this, da);
                    }
                case Sqlx.TIME: return new TTimeSpan(this, GetTime(lx, lx.pos));
                case Sqlx.TIMESTAMP: return new TDateTime(this, GetTimestamp(lx, lx.pos));
                case Sqlx.INTERVAL: return new TInterval(this, GetInterval(lx));
                case Sqlx.TYPE:
                case Sqlx.ROW:
                    {
                        if (lx.ch == '\0')
                            return defaultValue;
                        /*                       if (lx.mime == "text/xml")
                                               {
                                                   // tolerate missing values and use of attributes
                                                   var db = Database.Get(lx.tr, this);
                                                   var cols = new TypedValue[columns.Length];
                                                   var xd = new XmlDocument();
                                                   xd.LoadXml(new string(lx.input));
                                                   var xc = xd.FirstChild;
                                                   if (xc != null && xc is XmlDeclaration)
                                                       xc = xc.NextSibling;
                                                   if (xc == null)
                                                       goto bad;
                                                   bool blank = true;
                                                   for (int i = 0; i < columns.Length; i++)
                                                   {
                                                       var co = columns[i];
                                                       TypedValue item = null;
                                                       if (xc.Attributes != null)
                                                       {
                                                           var att = xc.Attributes[co.name.ToString()];
                                                           if (att != null)
                                                               item = co.Parse(lx.tr, att.InnerXml, lx.mime);
                                                       }
                                                       if (item == null)
                                                           for (int j = 0; j < xc.ChildNodes.Count; j++)
                                                           {
                                                               var xn = xc.ChildNodes[j];
                                                               if (xn.Name == columns[i].name.ToString())
                                                               {
                                                                   item = co.Parse(lx.tr, xn.InnerXml, lx.mime);
                                                                   break;
                                                               }
                                                           }
                                                       blank = blank && (item == null);
                                                       cols[i] = item;
                                                   }
                                                   if (blank)
                                                       return TXml.Null;
                                                   return new TRow(this, cols);
                                               }
                                               else */
                        if (lx.mime == "text/csv")
                        {
                            // we expect all columns, separated by commas, without string quotes
                            var cols = new TypedValue[Length];
                            for (int i = 0; i < Length; i++)
                            {
                                var co = columns[i];
                                var dt = co.domain;
                                TypedValue vl = null;
                                try
                                {
                                    switch (dt.kind)
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
                                                vl = dt.Parse(lx);
                                                break;
                                            }
                                        default: vl = dt.Parse(lx); break;
                                    }
                                }
                                catch (Exception)
                                {
                                    while (lx.ch != '\0' && lx.ch != ',' && lx.ch != '\r' && lx.ch != '\n')
                                        lx.Advance();
                                }
                                if (i < Length - 1)
                                {
                                    if (lx.ch != ',')
                                        throw new DBException("42101", lx.ch).Mix();
                                    lx.Advance();
                                }
                                else
                                {
                                    if (lx.ch == ',')
                                        lx.Advance();
                                    if (lx.ch != '\0' && lx.ch != '\r' && lx.ch != '\n')
                                        throw new DBException("42101", lx.ch).Mix();
                                    while (lx.ch == '\r' || lx.ch == '\n')
                                        lx.Advance();
                                }
                                cols[i] = vl;
                            }
                            return new TRow(this, cols);
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
                            var cols = new TColumn[Length];
                            var expect = BTree<string, int>.Empty;
                            for (int i = 0; i < Length; i++)
                            {
                                var co = columns[i];
                                expect += (co.name, i);
                                cols[i] = new TColumn(co.name, co.domain.defaultValue);
                            }
                            int j = 0;
                            bool namedOk = true;
                            bool unnamedOk = true;
                            lx.White();
                            while (lx.ch == comma)
                            {
                                lx.Advance();
                                lx.White();
                                var n = lx.GetName();
                                int? k = null;
                                if (n == null) // no name supplied
                                {
                                    if (!unnamedOk)
                                        throw new DBException("22208").Mix();
                                    namedOk = false;
                                    k = j++;
                                }
                                else // column name supplied
                                {
                                    if (lx.ch != ':')
                                        throw new DBException("42124").Mix();
                                    else
                                        lx.Advance();
                                    if (!namedOk)
                                        throw new DBException("22208").Mix()
                                            .Add(Sqlx.COLUMN_NAME, new TChar(n));
                                    unnamedOk = false;
                                    k = expect.Contains(n) ? expect[n] : -1; // includes ignoring _id
                                }
                                /*   if (k == null)
                                       throw new DBException("42112", n).Mix(); */
                                if (k == null)
                                    k = j++;
                                var ik = (int)k;
                                lx.White();
                                if (ik >= 0)
                                {
                                    var co = columns[ik];
                                    cols[ik] = new TColumn(co.name, co.domain.Parse(lx));
                                }
                                comma = ',';
                                lx.White();
                            }
                            if (lx.ch != end)
                                break;
                            lx.Advance();
                            return new TRow(cols);
                        }
                    }
                case Sqlx.TABLE:
                    {
                        return ParseList(lx);
                    }
                case Sqlx.ARRAY:
                    {
                        return elType.ParseList(lx);
                    }
                case Sqlx.UNION:
                    {
                        int st = lx.pos;
                        char ch = lx.ch;
                        for (var b = mem.First(); b != null; b = b.Next())
                            if (b.value() is Domain dt)
                            {
                                try
                                {
                                    var v = dt.Parse(lx, true);
                                    lx.White();
                                    if (lx.ch == ']' || lx.ch == ',' || lx.ch == '}')
                                        return v;
                                }
                                catch (Exception) { }
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
                        var gps = BTree<string, bool>.Empty;
                        var rfs = BTree<string, bool>.Empty;
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
                        return TLevel.New(new Level((byte)min, (byte)max, gps, rfs));
                    }
            }
#if !WINDOWS_PHONE
        bad:
#endif
            if (lx.pos + 4 < lx.len && new string(lx.input, start, 4).ToLower() == "null")
            {
                for (int i = 0; i < 4; i++)
                    lx.Advance();
                return TNull.Value;
            }
            var xs = new string(lx.input, start, lx.pos - start);
            throw new DBException("22005E", ToString(), xs).ISO()
                .AddType(this).AddValue(new TChar(xs));
        }
        TypedValue ParseList(Scanner lx)
        {
            if (kind == Sqlx.SENSITIVE)
                return new TSensitive(this, elType.ParseList(lx));
            var rv = new TArray(this);
            int j = 0;
            /*            if (lx.mime == "text/xml")
                        {
                            var xd = new XmlDocument();
                            xd.LoadXml(new string(lx.input));
                            for (int i = 0; i < xd.ChildNodes.Count; i++)
                                rv[j++] = Parse(lx.tr, xd.ChildNodes[i].InnerXml);
                        }
                        else
            */
            {
                char delim = lx.ch, end = ')';
                if (delim == '[')
                    end = ']';
                if (delim != '(' && delim != '[')
                {
                    var xs = new string(lx.input, 0, lx.len);
                    throw new DBException("22005F", ToString(), xs).ISO()
                        .AddType(this).AddValue(new TChar(xs));
                }
                lx.Advance();
                for (; ; )
                {
                    lx.White();
                    if (lx.ch == end)
                        break;
                    rv[j++] = Parse(lx);
                    if (lx.ch == ',')
                        lx.Advance();
                    lx.White();
                }
                lx.Advance();
            }
            return rv;
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
            string[] parts = GetParts(lx, ke - ks, st);
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
        public TypedValue Coerce(TypedValue v)
        {
            if (this == Null || this==Content)
                return v;
            for (var b = constraints?.First(); b != null; b = b.Next())
                if (b.value().Eval(null,null) != TBool.True)
                    throw new DBException("22211",name);
            if (v is TSensitive st)
            {
                if (kind == Sqlx.SENSITIVE)
                    return elType.Coerce(st.value);
                throw new DBException("22210");
            }
            if (kind == Sqlx.SENSITIVE)
                return elType.Coerce(v);
            if (v == null || v.IsNull)
                return v;
            if (abbrev != null && v.dataType.kind == Sqlx.CHAR && kind != Sqlx.CHAR)
                v = Parse(new Scanner(v.ToString().ToCharArray(), 0));
            if (CompareTo(v.dataType) == 0)
                return v;
            if (kind == Sqlx.ROW && Length == 0 || (Length == 1 && v.dataType.Length == 0 && columns[0].domain.HasValue(v)))
                return v;
            var vk = Equivalent(v.dataType.kind);
            if (vk == Sqlx.ROW && v.dataType.Length == 1
                && CompareTo(v.dataType.columns[0]) == 0)
                return (v as TRow)[0];
            if (kind == Sqlx.TYPE && vk == Sqlx.DOCUMENT)
            {
                var doc = v as TDocument;
                var obs = new TypedValue[Length];
                for (var b = columns.First(); b != null; b = b.Next())
                {
                    var tc = b.value();
                    obs[tc.seq] = (doc[tc.name] is TypedValue f) ?
                        tc.domain.Coerce(f) : tc.domain.defaultValue;
                }
                return new TRow(this, obs);
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
                                int r = 0;
                                while (a.scale > 0)
                                {
                                    a.mantissa = a.mantissa.Quotient(10, ref r);
                                    a.scale--;
                                }
                                while (a.scale < 0)
                                {
                                    a.mantissa = a.mantissa.Times(10);
                                    a.scale++;
                                }
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
                                while (a.scale > scale)
                                {
                                    a.mantissa = a.mantissa.Quotient(10, ref r);
                                    a.scale--;
                                }
                                while (a.scale < scale)
                                {
                                    a.mantissa = a.mantissa.Times(10);
                                    a.scale++;
                                }
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
                                default: str = v.ToString(); break;
                            }
                            if (prec != 0 && str.Length > prec)
                                throw new DBException("22001", "CHAR(" + prec + ")", "CHAR(" + str.Length + ")").ISO()
                                                    .AddType(this).AddValue(vt);
                            return new TChar(this, str);
                        }
                    case Sqlx.ROW:
                        if (vk == Sqlx.ROW)
                        {
                            if (Length != v.dataType.Length)
                                break;
                            TRow vr = v as TRow;
                            var nc = new TypedValue[Length];
                            bool usenc = false;
                            for (int i = 0; i < Length; i++)
                            {
                                TypedValue tt = vr[i];
                                if (tt == null || !tt.dataType.EqualOrStrongSubtypeOf(columns[i].domain))
                                    tt = columns[i].domain.Coerce(tt);
                                if (tt != vr[i])
                                {
                                    var cc = columns[i];
                                    nc[i] = tt;
                                    usenc = true;
                                }
                                else
                                    nc[i] = vr[i];
                            }
                            if (usenc)
                                return new TRow(this, nc);
                            return new TRow(this, vr);
                        }
                        break;
                    case Sqlx.PERIOD:
                        {
                            var pd = v.Val() as Period;
                            return new TPeriod(this, new Period(elType.Coerce(pd.start), elType.Coerce(pd.end)));
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
#if MONGO
                    case Sqlx.OBJECT:
                        {
                            switch (v.dataType.kind)
                            {
                                case Sqlx.BLOB: return new TObjectId((byte[])v.Val());
                                case Sqlx.CHAR: return new TObjectId(cx,v.ToString());
                            }
                            break;
                        }
#endif
                    case Sqlx.DOCARRAY: goto case Sqlx.DOCUMENT;
#if SIMILAR
                    case Sqlx.REGULAR_EXPRESSION:
                        {
                            switch (v.DataType.kind)
                            {
                                case Sqlx.CHAR: return new TChar(v.ToString());
                            }
                            break;
                        }
#endif
                    case Sqlx.VALUE:
                    case Sqlx.NULL:
                        return v;
                    default:
                        for (Domain t = this; t != null; t = t.super)
                            if (t.HasValue(v))
                                return v;
                        break;
                }
            throw new DBException("22005G", this, v.ToString()).ISO();
        }
        internal TypedValue Coerce(RowBookmark rb)
        {
            var q = rb._rs.qry;
            var v = rb.row;
            if (Length == 0 && q.display == 1)
                return Coerce(v[0]);
            if (v.Length > q.display)
                v = new TRow(q.rowType,v.values);
            Coerce(v);
            return v;
        }
        internal TypedValue Fix(TypedValue v)
        {
            if (v == null)
                v = defaultValue;
            else if (this != v.dataType)
                v = Coerce(v);
            return v;
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
                    case Sqlx.ONLY: return super.SystemType;
                    case Sqlx.NULL: return typeof(DBNull);
                    case Sqlx.INTEGER: return typeof(long);
                    case Sqlx.NUMERIC: return typeof(Decimal);
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
        /// Select a predefined data type
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
                case Sqlx.REAL: return Real;
                case Sqlx.REALLITERAL: return Real;
                case Sqlx.TIME: return Timespan;
                case Sqlx.TIMESTAMP: return Timestamp;
            }
            return new Domain(t, BTree<long, object>.Empty);
        }
        /// <summary>
        /// Validator
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public bool Check(string s)
        {
            if (kind == Sqlx.SENSITIVE)
                return elType.Check(s);
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
        /// <summary>
        /// Evaluate a binary operation 
        /// </summary>
        /// <param name="a">The first object</param>
        /// <param name="op">The binary operation</param>
        /// <param name="b">The second object</param>
        /// <returns>The evaluated object</returns>
        public virtual TypedValue Eval(TypedValue a, Sqlx op, TypedValue b) // op is + - * / so a and b should be compatible arithmetic types
        {
            if (kind == Sqlx.SENSITIVE)
                return new TSensitive(this, elType.Eval(a, op, b));
            if (op == Sqlx.NO)
                return Coerce(a);
            if (a == null || a.IsNull || b == null || b.IsNull)
                return defaultValue;
            if (a is TUnion)
                a = ((TUnion)a).LimitToValue(); // a coercion possibly
            if (b is TUnion)
                b = ((TUnion)b).LimitToValue();
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
                        return Eval(b, op, a);
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
                    if (a.dataType.Constrain(Int) != null)
                        a = new TNumeric(new Numeric(a.ToInteger(), 0));
                    if (b.dataType.Constrain(Int) != null)
                        b = new TNumeric(new Numeric(b.ToInteger(), 0));
                    if (a is TNumeric && b is TNumeric)
                        return new TNumeric(DecimalOps(((TNumeric)a).value, op, ((TNumeric)b).value));
                    var ca = a.ToDouble();
                    var cb = b.ToDouble();
                    return Coerce(new TReal(this, DoubleOps(ca, op, cb)));
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
                        Interval ic = null;
                        switch (bk)
                        {
                            case Sqlx.DATE:
                                return Eval(b, op, a);
                            case Sqlx.INTEGER:
                                var bi = b.ToInt().Value;
                                if (ia.yearmonth)
                                {
                                    var m = ia.years * 12 + ia.months;
                                    ic = new Interval(0, 0);
                                    switch (kind)
                                    {
                                        case Sqlx.TIMES: m = m * bi; break;
                                        case Sqlx.DIVIDE: m = m / bi; break;
                                    }
                                    if (start == Sqlx.YEAR)
                                    {
                                        ic.years = m / 12;
                                        if (end == Sqlx.MONTH)
                                            ic.months = m - 12 * (m / 12);
                                    }
                                    else
                                        ic.months = m;
                                    return new TInterval(this, ic);
                                }
                                break;
                            case Sqlx.INTERVAL:
                                var ib = (Interval)b.Val();
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
                    return Coerce(elType.Eval(a, op, b));
            }
            throw new DBException("22005L", kind, a).ISO();
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
        public Domain Path(Ident field)
        {
            if (kind == Sqlx.SENSITIVE)
                return elType.Path(field);
            if (field == null || kind == Sqlx.CONTENT)
                return this;
            if (kind == Sqlx.DOCUMENT)
                return Content;
            if ((kind == Sqlx.TYPE || kind == Sqlx.ROW) && Length > 0)
            {
                for (var b = columns.First(); b != null; b = b.Next())
                    if (b.value().name == field.ident)
                    {
                        var r = columns[b.key()];
                        if (r != null)
                            field.Set(b.value().defpos);
                        if (field.sub != null)
                            return r.domain.Path(field.sub);
                        return r.domain;
                    }
            }
            return null;
        }
        internal TypedValue Copy(TypedValue v)
        {
            if (v == null || v.IsNull)
                return defaultValue;
            switch (Equivalent(kind))
            {
                case Sqlx.CONTENT:
                case Sqlx.VALUE:
                case Sqlx.Null:
                    return v;
                case Sqlx.ARRAY:
                    if (v is TArray)
                        return new TArray(this, ((TArray)v).list);
                    break;
                case Sqlx.INTEGER:
                    if (v is TInt)
                        return new TInt(this, ((TInt)v).value);
                    if (v is TInteger)
                        return new TInt(this, (long)((TInteger)v).ivalue);
                    break;
                case Sqlx.NUMERIC:
                    if (v is TNumeric)
                        return new TNumeric(this, ((TNumeric)v).value);
                    if (v is TInt)
                        return new TNumeric(this, new Numeric(((TInt)v).value.Value));
                    break;
                case Sqlx.REAL:
                    if (v is TReal)
                    {
                        var r = v as TReal;
                        if (((object)r.nvalue) == null)
                            return new TReal(this, r.dvalue);
                        return new TReal(this, r.nvalue);
                    }
                    if (v is TNumeric)
                        return new TReal(this, ((TNumeric)v).value);
                    if (v is TInt)
                        return new TReal(this, (((TInt)v).value.Value));
                    break;
                case Sqlx.NCHAR:
                case Sqlx.CHAR:
                    if (v is TChar)
                        return new TChar(this, ((TChar)v).value);
                    break;
                case Sqlx.BOOLEAN:
                    if (v is TBool)
                        return v;
                    break;
                case Sqlx.DATE:
                case Sqlx.TIMESTAMP:
                    if (v is TDateTime)
                        return new TDateTime(this, ((TDateTime)v).value.Value);
                    if (v is TInt)
                    {
                        var w = v.ToLong();
                        if (w > DateTime.MaxValue.Ticks)
                            w = DateTime.MaxValue.Ticks;
                        return new TDateTime(this, new DateTime(w.Value));
                    }
                    break;
                case Sqlx.TIME:
                    if (v is TTimeSpan)
                        return new TTimeSpan(this, ((TTimeSpan)v).value);
                    break;
                case Sqlx.INTERVAL:
                    if (v is TInterval)
                        return new TInterval(this, ((TInterval)v).value);
                    break;
                case Sqlx.MULTISET:
                    if (v is TMultiset)
                        return new TMultiset(this, (CTree<TypedValue, long?>)(((TMultiset)v).tree));
                    break;
                case Sqlx.DOCUMENT:
                    if (v is TDocument)
                        return v;
                    break;
                case Sqlx.DOCARRAY:
                    if (v is TDocArray)
                        return v;
                    break;
                case Sqlx.PERIOD:
                    if (v is TPeriod)
                        return new TPeriod(this, ((TPeriod)v).value);
                    break;
                case Sqlx.TYPE:
                    if (v is TRow)
                        return new TRow(this, (TRow)v);
                    break;
                case Sqlx.ROW:
                    if (v is TRow)
                        return new TRow(this, (TRow)v);
                    break;
                case Sqlx.TABLE:
                    if (v is TRowSet)
                        return v;
                    break;
                case Sqlx.UNION:
                    return v.dataType.Copy(v);
            }
            throw new DBException("22005D", this.ToString(), v.dataType.ToString()).ISO();
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
                case Sqlx.SENSITIVE: return elType.BsonType();
            }
        }
        /// <summary>
        /// Compute the datatype resulting from limiting this by another datatype constraint.
        /// this.LimitBy(union) gives this if this is in the union, otherwise
        /// this.LimitBy(dt) gives the same result as dt.LimitBy(this).
        /// </summary>
        /// <param name="dt"></param>
        /// <returns></returns>
        internal Domain Constrain(Domain dt)
        {
            if (kind == Sqlx.SENSITIVE)
            {
                if (dt.kind == Sqlx.SENSITIVE)
                {
                    var ts = elType.Constrain(dt.elType);
                    if (ts == null)
                        return null;
                    return ts.Equals(elType) ? this : ts.Equals(dt.elType) ? dt :
                        new Domain(Sqlx.SENSITIVE, ts);
                }
                var tt = elType.Constrain(dt);
                if (tt == null)
                    return null;
                return tt.Equals(elType) ? this : new Domain(Sqlx.SENSITIVE, tt);
            }
            if (dt.kind == Sqlx.SENSITIVE)
            {
                var tu = Constrain(dt.elType);
                if (tu == null)
                    return null;
                return tu.Equals(dt.elType) ? dt : new Domain(Sqlx.SENSITIVE, tu);
            }
            if (dt == null || dt == Null)
                return this;
            var ki = Equivalent(kind);
            var dk = Equivalent(dt.kind);
            var r = this;
            if ((ki == Sqlx.ARRAY || ki == Sqlx.MULTISET) && ki == dk && dt.elType == null)
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
            if (dk == Sqlx.ONLY && Equals(dt.super))
                return dt;
            if (ki == Sqlx.ONLY && super.Equals(dt))
                return this;
            if (kind == Sqlx.PASSWORD && dt.kind == Sqlx.CHAR)
                return this;
            if (ki == dk && (kind == Sqlx.ARRAY || kind == Sqlx.MULTISET))
            {
                if (elType == null)
                    return dt;
                var ect = elType.Constrain(dt.elType);
                if (ect == elType)
                    return this;
                return dt;
            }
            if (ki == Sqlx.UNION && dk != Sqlx.UNION)
                for (var b = mem.First(); b != null; b = b.Next())
                    if (b.value() is Domain dm && dm.Constrain(dt) != null)
                        return dm;
            if (ki != Sqlx.UNION && dk == Sqlx.UNION)
                for (var b = dt.mem.First(); b != null; b = b.Next())
                    if (b.value() is Domain dm && dm.Constrain(this) != null)
                        return this;
            if (ki == Sqlx.UNION && dk == Sqlx.UNION)
            {
                var nt = BTree<long, object>.Empty;
                for (var b = mem.First(); b != null; b = b.Next())
                    if (b.value() is Domain db)
                        for (var c = dt.mem.First(); c != null; c = c.Next())
                            if (c.value() is Domain dc)
                            {
                                var u = db.Constrain(dc);
                                if (u != null)
                                    nt += (u.defpos, u);
                            }
                if (nt.Count == 0)
                    return null;
                if (nt.Count == 1)
                    return (Domain)nt.First().value();
                return new Domain(Sqlx.UNION, nt);
            }
            else if (elType != null && dt.elType != null)
                r = new Domain(kind, elType.LimitBy(dt.elType));
            else if (ki == Sqlx.ROW && dt == TableType)
                return this;
            else if ((ki == Sqlx.ROW || ki == Sqlx.TYPE) && (dk == Sqlx.ROW || dk == Sqlx.TABLE))
            {
                if (columns == null)
                    r = dt;
                else if (dt.columns == null)
                    r = this;
                else
                {
                    if (Length != dt.Length)
                        return null;
                    var cs = BList<Selector>.Empty;
                    for (int i = 0; i < Length; i++)
                    {
                        var dc = columns[i].domain.Constrain(dt.columns[i].domain);
                        if (dc == null)
                            return null;
                        cs += (i, new Selector(columns[i].name, columns[i].defpos, dc, i));
                    }
                    r = new Domain(cs);
                }
                return r;
            }
            else if ((ki != Sqlx.ROW && ki != dk) || (ki == Sqlx.ROW && dk != Sqlx.ROW) ||
                    (elType == null) != (dt.elType == null) || orderFunc != dt.orderFunc || orderflags != dt.orderflags)
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
                if (dt.iri != "" && dt.iri != r.iri)
                    m += (Iri, dt.iri);
                else if (iri != "" && iri != r.iri)
                    m += (Iri, r.iri);
                if (dt.charSet != CharSet.UCS && dt.charSet != r.charSet)
                    m += (Charset, dt.charSet);
                else if (charSet != CharSet.UCS && charSet != r.charSet)
                    m += (Charset, r.charSet);
                if (dt.culture != CultureInfo.InvariantCulture && dt.culture != r.culture)
                    m += (Culture, dt.culture);
                else if (culture != CultureInfo.InvariantCulture && culture != r.culture)
                    m += (Culture, r.culture);
                if (dt.defaultValue != TNull.Value && dt.defaultValue != r.defaultValue)
                    m += (Default, dt.defaultValue);
                else if (defaultValue != TNull.Value && defaultValue != r.defaultValue)
                    m += (Default, r.defaultValue);
                if (dt.search != null && dt.search != r.search)
                    m += (Search, dt.search);
                else if (search != null && search != r.search)
                    m += (Search, r.search);
                r = new Domain(r.kind, m);
            }
            return r;
        }
        internal Domain LimitBy(Domain dt)
        {
            return Constrain(dt) ?? this;
        }

        internal override Basis New(BTree<long, object> m)
        {
            return new Domain(kind, defpos, m);
        }

        internal static TypedValue Now => new TDateTime(Timestamp, DateTime.Now);
        internal static TypedValue MaxDate => new TDateTime(Timestamp, DateTime.MaxValue);
        /// <summary>
        /// Provide an XML version of the type information for the client
        /// </summary>
        /// <param name="transaction"></param>
        /// <returns>an XML string</returns>
        internal string Info()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("<PyrrhoDBType kind=\"" + kind + "\" ");
            if (iri != null)
                sb.Append("iri=\"" + iri + "\" ");
            bool empty = true;
            if (kind == Sqlx.ONLY)
                sb.Append(name);
            if (elType != null)
                sb.Append("elType=" + elType + "]");
            if (AscDesc != Sqlx.NULL)
                sb.Append("," + AscDesc);
            if (Nulls != Sqlx.NULL)
                sb.Append("," + Nulls);
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
            if (search != null)
                sb.Append(",C=" + search);
            if (abbrev != null)
                sb.Append(",A=" + abbrev);
            if (empty)
                sb.Append("/>");
            return sb.ToString();
        }
        /// <summary>
        /// Decide which type we want for a union type and an actual value
        /// </summary>
        /// <param name="v">the value</param>
        /// <returns>the type from the union appropriate for this value</returns>
        internal Domain TypeOf(Context cx, TypedValue v)
        {
            if (kind == Sqlx.SENSITIVE)
                return new Domain(Sqlx.SENSITIVE, elType.TypeOf(cx, v));
            if (kind != Sqlx.UNION)
                return this;
            int n = (int)columns.Count;
            for (int j = 0; j < n; j++)
            {
                var r = columns[j].domain;
                if (r.CanTakeValueOf(v.dataType))
                    return r;
            }
            return null;
        }
        /// <summary>
        /// Create an XML string for a given value
        /// </summary>
        /// <param name="tr">The transaction: Common doesn't know about this</param>
        /// <param name="ob">the value to represent</param>
        /// <returns>the corresponding XML</returns>
        public virtual string Xml(Transaction tr,Context cx,long defpos, TypedValue ob)
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
                        for (int j = 0; j < a.Length; j++)
                            sb.Append("<item " + elType.Xml(tr,cx, elType.defpos, a[j]) + "</item>");
                        break;
                    }
                case Sqlx.MULTISET:
                    {
                        var m = (TMultiset)ob;
                        //          sb.Append("type=\"multiset\">");

                        for (var e = m.tree.First(); e != null; e = e.Next())
                            sb.Append("<item " + elType.Xml(tr,cx, elType.defpos, e.key()) + "</item>");
                        break;
                    }
                case Sqlx.ROW:
                case Sqlx.TABLE:
                case Sqlx.TYPE:
                    {
                        TRow r = (TRow)ob;
                        if (r.dataType.names.Count == 0)
                            throw new DBException("2200N").ISO();
                        var ro = tr.role;
                        var sc = sb;
                        if (ro != null)
                            sb.Append("<" + ro.name);
                        var ss = new string[r.dataType.Length];
                        var empty = true;
                        for (int i = 0; i < r.dataType.Length; i++)
                        {
                            var tv = r[i];
                            if (tv == null)
                                continue;
                            var kn = r.dataType.columns[i].defpos;
                            var p = tv.dataType;
                            var m = (ro.objects[kn] as DBObject).Meta();
                            if (tv != null && !tv.IsNull && m != null && m.Has(Sqlx.ATTRIBUTE))
                                sb.Append(" " + kn + "=\"" + tv.ToString() + "\"");
                            /*                          else if (tv!=null && !tv.IsNull && m != null && m.Has(Sqlx.REFERS))
                                                      {
                                                          var rc = m as RoleObject;
                                                          for (var xp = d.indexes.First();xp!= null; xp = xp.Next())
                                                          {
                                                              var x = d.GetObject(xp.key()) as Index;
                                                              if (x.tabledefpos != defpos)
                                                                  continue;
                                                              if (x.cols.Length != 1 || x.cols[0] != mp.Value)
                                                                  continue;
                                                              var rx = d.GetObject(x.refindexdefpos) as Index;
                                                              var rt = d.GetObject(x.reftabledefpos) as Table;
                                                              var fm = new From(d._Context, "X"+x.reftabledefpos+":"+i, rt);
                                                              var irs = new IndexRowSet(d._Context.transaction,fm, rx, null);
                                                              ss[i] = fm.nominalDataType.Xml(d, x.reftabledefpos, irs.First()?[i]);
                                                              empty = false;
                                                              break;
                                                          }
                                                      } 
                                                      else if (m != null && m.Has(Sqlx.REFERRED))
                                                      {
                                                          var rc = m as RoleObject;
                                                          for (var xp = d.indexes.First(); xp != null; xp = xp.Next())
                                                          {
                                                              var x = d.GetObject(xp.key()) as Index;
                                                              if (x.reftabledefpos != defpos)
                                                                  continue;
                                                              if (x.cols.Length != 1 || d._Role.defs[x.cols[0]].name.ToString() != kn.ToString())
                                                                  continue;
                                                              var rx = d.GetObject(x.refindexdefpos) as Index;
                                                              var rk = d._Role.defs[rx.cols[0]].name;
                                                              if (rk == null)
                                                                  throw new DBException("42102");
                                                              var it = d.GetObject(x.tabledefpos) as Table;
                                                              var fm = new From(d._Context, "X"+x.tabledefpos+":"+i, it);
                                                              var irs = new IndexRowSet(d._Context.transaction,fm, x, null);
                                                              fm.rowSet = irs;
                                                              var sr = new StringBuilder();
                                                              for (var ia = irs.First(); ia != null; ia = ia.Next())
                                                                  sr.Append(irs.rowType.Xml(d, x.tabledefpos, ia[i]));
                                                              ss[i] = sr.ToString();
                                                              empty = false;
                                                              break;
                                                          }
                                                      } */
                            else if (tv != null && !tv.IsNull)
                            {
                                ss[i] = "<" + kn + " type=\"" + p.ToString() + "\">" +
                                    p.Xml(tr,cx, defpos, tv) + "</" + kn + ">";
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

    }
    internal class StandardDataType : Domain
    {
        public static BTree<Sqlx, Domain> types = BTree<Sqlx, Domain>.Empty;
        internal StandardDataType(Sqlx t, Domain o = null, string c = null, BTree<long, object> u = null)
            : base(t, _Mem(o, c, u))
        {
            types += (t, this);
        }
        static BTree<long, object> _Mem(Domain o, string c, BTree<long, object> u)
        {
            if (u == null)
                u = BTree<long, object>.Empty;
            if (o != null)
                u += (Element, o);
            if (c != null)
                u += (Search, c);
            return u;
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
            if (wr.db.levels.Contains(lev))
                wr.PutLong(wr.db.levels[lev]);
            else
            {
                wr.db += (lev,wr.Length);
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
        internal static Level DeserialiseLevel(Reader rdr)
        {
            Level lev;
            var lp = rdr.GetLong();
            if (lp != -1)
                lev =rdr.db.cache[lp];
            else
            {
                var min = (byte)rdr.ReadByte();
                var max = (byte)rdr.ReadByte();
                var gps = BTree<string, bool>.Empty;
                var n = rdr.GetInt();
                for (var i = 0; i < n; i++)
                    gps += (rdr.GetString(), true);
                var rfs = BTree<string, bool>.Empty;
                n = rdr.GetInt();
                for (var i = 0; i < n; i++)
                    rfs += (rdr.GetString(), true);
                lev = new Level(min, max, gps, rfs);
                rdr.db += (lev, lp);
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
    internal class Period
    {
        public TypedValue start, end;
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
        /// the current character
        /// </summary>
        internal char ch;
        /// <summary>
        /// Whether to use XML conventions
        /// </summary>
        internal string mime = "text/plain";
        /// <summary>
        /// Constructor: prepare the scanner
        /// Invariant: ch==input[pos]
        /// </summary>
        /// <param name="s">the input array</param>
        /// <param name="p">the starting position</param>
        internal Scanner(char[] s, int p)
        {
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
        internal Scanner(char[] s, int p, string m)
        {
            input = s;
            mime = m;
            len = input.Length;
            pos = p;
            ch = (p < len) ? input[p] : '\0';
        }
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
    internal class Idents
    {
        public readonly BTree<Ident, int?> _names;
        public readonly BList<Ident> _list;
        public readonly BTree<long, int?> _bypos;
        public Idents()
        {
            _names = Ident.IdTree<int?>.Empty;
            _list = BList<Ident>.Empty;
            _bypos = BTree<long, int?>.Empty;
        }
        protected Idents(BTree<Ident,int?> n, BList<Ident>ls,BTree<long,int?> b)
        {
            _names = n;
            _list = ls;
            _bypos = b;
        }
        internal Ident this[int i] => _list[i];
        public static Idents operator+ (Idents x,(int,Ident)v)
        {
            var ls = x._list;
            var n = x._names;
            var b = x._bypos;
            while (v.Item1 > (int)ls.Count)
                ls += ((int)ls.Count, null);
            if (ls[v.Item1] is Ident id)
                n -= id;
            ls += (v.Item1, v.Item2);
            n += (v.Item2, v.Item1);
            if (v.Item2.segpos > 0)
                b += (v.Item2.segpos, v.Item1);
            return new Idents(n, ls, b);
        }
        /// <summary>
        /// Find the position in the list of an Ident, allowing a partial match
        /// </summary>
        /// <param name="n">The Ident to look up</param>
        /// <param name="s">The unused portion of n</param>
        /// <returns>The position found</returns>
        internal (Idents,int?) Get(Ident n, out Ident s)
        {
            s = null;
            if (n == null)
                return (this,null);
            var x = this;
            int? iq = null;
            for (var bm = _names.First(); iq == null && bm != null; bm = bm.Next())
            {
                var k = bm.key();
                if (k.ident.CompareTo(n.ident) != 0)
                    continue;
                iq = bm.value();
                if (iq == null && n.segpos > 0)
                {
                    iq = _bypos[n.segpos];
                    if (iq.HasValue)
                        break;
                    var ss = "" + n.segpos;
                    for (var b = _names.First(); b != null; b = b.Next())
                        if (b.key().ident == ss)
                        {
                            x += (n, b.value());
                            break;
                        }
                }
            }
            if (iq.HasValue)
            {
                var id = _list[iq.Value];
                while (n != null && id != null && n.ident == id.ident)
                {
                    s = n.sub;
                    n = s;
                    id = id.sub;
                }
            }
            return (x,iq);
        }
        /// <summary>
        /// As abovem but prefix n by q's alias
        /// </summary>
        /// <param name="q">The query whose alias may have been used</param>
        /// <param name="n">The identifier we are after</param>
        /// <param name="s">The subscript to return if any</param>
        /// <returns>The position of Q.N in our list</returns>
        internal int? Get(Query q, Ident n, out Ident s)
        {
            var qn = q.name;
            s = null;
            if (n == null)
                return null;
            int? iq = null;
            Ident id = null;
            for (var bm = _names.First(); bm != null; bm = bm.Next())
            {
                var k = bm.key();
                if (k.ident.CompareTo(qn) == 0)
                {
                    iq = bm.value();
                    id = _list[iq.Value].sub;
                    if (id != null && id.ident.CompareTo(n.ident) == 0)
                        break;
                }
                if (k.CompareTo(n) > 0)
                    break;
            }
            if (iq.HasValue)
            {
                while (n != null && id != null && n.ident == id.ident)
                {
                    s = n.sub;
                    n = s;
                    id = id.sub;
                }
            }
            return iq;
        }
        public static Idents operator+ (Idents x,(Ident,int?) v)
        {
            return new Idents(x._names, x._list, x._bypos + (v.Item1.segpos, v.Item2));
        }
        public static Idents operator+(Idents x,Ident n)
        {
            var ns = x._names;
            var ls = x._list;
            var b = x._bypos;
            var m = (int)ls.Count;
            ns += (n,m);
            ls += (m, n);
            x = new Idents(ns, ls, x._bypos);
            if (n.Defpos() > 0)
                x += (n, m);
            return x;
        }
        /// <summary>
        /// As above but require an exact match
        /// </summary>
        /// <param name="n"></param>
        /// <returns></returns>
        internal (Idents,int?) Find(Ident n)
        {
            var x = this;
            if (n == null)
                return (this,null);
            int? iq = _names[n];
            if (iq == null && n.segpos > 0)
            {
                iq = _bypos[n.segpos];
                if (iq.HasValue)
                    return (this,iq);
                var ss = "" + n.segpos;
                for (var b = _names.First(); b != null; b = b.Next())
                    if (b.key().ident == ss)
                    {
                        x += (n, b.value());
                        break;
                    }
                if (iq.HasValue && _list[iq.Value].CompareTo(n) != 0)
                    return (x,null);
            }
            if (iq == null)
                return (x,null);
            var nm = _list[iq.Value];
            if (nm.segpos > 0 && n.segpos > 0 && nm.segpos != n.segpos)
                return (x,null);
            return (x,iq);
        }
        internal int? this[string s]
        {
            get
            {
                long p = -1;
                if (s != "" && Char.IsDigit(s[0]))
                    p = long.Parse(s);
                var iq = _bypos[p];
                if (iq.HasValue)
                    return iq;
                for (var a = _list.First(); a != null; a = a.Next())
                {
                    var v = a.value();
                    if (v.Final().ident == s || p == v.segpos)
                        return a.key();
                }
                return null;
            }
        }

        internal int Length
        {
            get { return (int)_list.Count; }
        }
        internal int ForPos(long cp)
        {
            var iq = _bypos[cp];
            if (iq.HasValue)
                return iq.Value;
            for (int i = 0; i < Length; i++)
                if (_list[i].Defpos() == cp)
                    return i;
            return -1;
        }
        internal Idents Replace(int i, Ident n)
        {
            var r = new Idents(_names -_list[i]+(n,i), _list+(i,n), _bypos);
            if (n.segpos > 0)
                r += (n, i);
            return r;
        }
        internal void Clean()
        {
            for (int i = 0; i < Length; i++)
                _list[i].Clean();
        }

        internal (Idents,bool) Match(Idents n)
        {
            if (n?.Length != Length)
                return (this,false);
            for (var i = 0; i < Length; i++)
            {
                int? j;
                (n, j) = n.Find(_list[i]);
                if (j != i)
                    return (n,false);
            }
            return (n,true);
        }
        internal Idents ForTableType()
        {
            for (var i = 0; i < Length; i++)
                if (this[i].sub != null)
                    goto doit;
            return this;
        doit:
            var r = new Idents();
            for (var i = 0; i < Length; i++)
                r+=this[i].ForTableType();
            return r;
        }
        public override string ToString()
        {
            var sb = new StringBuilder("(");
            var cm = "";
            for (var i = 0; i < Length; i++)
            {
                sb.Append(cm); cm = ",";
                sb.Append(_list[i]);
            }
            sb.Append(')');
            return sb.ToString();
        }
    }
    /// <summary>
    /// A class for RdfLiterals
    /// </summary>
    internal class RdfLiteral : TChar
    {
        public object val; // the binary version
        public bool name; // whether str matches val
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
}
