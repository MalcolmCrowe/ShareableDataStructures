using System.Text;
using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System;
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
    /// Level 3 Role object. 
    /// Roles manage grantable and renamable role-based database objects: 
    /// roles/users, table/views, procedures, domains/UDTypes.
    /// These also have ObInfo and Selector for giving names/metadata and rowTypes of these objects.
    /// Roles also manage granted permissions for these and for database, role, TableColumn:
    /// If ob is not User, role.obinfos[ob.defpos] contains the privilege that this role has on ob.
    /// If us is a User, then role.obinfo[us.defpos] tells whether the user has usage rights on the role.
    /// Granted permissions on columns affects rowTypes of the container objects.
    /// All of the above can be looked up by name (dbobjects[], procedures[]) and by defpos (obinfos[]).
    /// Procedure lookup is complicated by having different name space for each arity.
    /// Objects not in the above lists can have name and rowType directly in the object structure,
    /// and this is also true for SystemTables.
    /// ALTER ROLE statements can only be executed by the role or its creator.
    /// Two special implicit roles cannot be ALTERed.
    /// The schema role has access to system tables, and is the initial role for a new database.
    /// The database owner is always allowed to use the schema role, but it is otherwise
    /// subject to normal SQL statements.
    /// The guest role has access to all such that have been granted to PUBLIC: there is no Role object for this.
    /// In Pyrrho users are granted roles, and roles can be granted objects.  
    /// There is one exception: a single user can be granted ownership of the database.
    /// If a role is granted to another role it causes a cascade of permissions, but does not establish
    /// an ongoing relationship between the roles (granting a role to a user does). 
    /// For ease of implementation, the transaction's Role also holds ObInfos for SqlValues and Queries. 
    /// Immutable
    /// </summary>
    internal class Role : DBObject
    {
        internal readonly static long
            DBObjects = -248, // BTree<string,long> Domain/Table/View etc by name
            Procedures = -249; // BTree<string,BList<long>> Procedure/Function by name and arity
        public string name => (string)(mem[Name] ?? "");
        internal BTree<string, long> dbobjects => 
            (BTree<string, long>)mem[DBObjects]??BTree<string,long>.Empty;
        internal BTree<string, BTree<int,long>> procedures => // not BList<long> !
            (BTree<string, BTree<int,long>>)mem[Procedures]??BTree<string,BTree<int,long>>.Empty;
        public BTree<long, object> obinfos => mem;
        public const Grant.Privilege use = Grant.Privilege.UseRole,
            admin = Grant.Privilege.UseRole | Grant.Privilege.AdminRole;
        /// <summary>
        /// An empty role for the Context (query analysis)
        /// </summary>
        /// <param name="dp"></param>
        internal Role(long dp) : base(dp, BTree<long, object>.Empty) { }
        /// <summary>
        /// Just to create the schema and guest roles
        /// </summary>
        /// <param name="nm"></param>
        /// <param name="u"></param>
        internal Role(string nm, long defpos, BTree<long, object> m)
            : base(nm, defpos, defpos, -1, m
                  + (DBObjects, BTree<string, long>.Empty)
                  + (Procedures, BTree<string, BTree<int,long>>.Empty))
        { }
        public Role(PRole p, Database db, bool first)
            : base(p.name, p.ppos, p.ppos, db.role.defpos,
                 (first ? db.schemaRole.mem : db.guestRole.mem)
                 + (DBObjects, first ? db.schemaRole.dbobjects : db.guestRole.dbobjects)
                 + (Procedures, first ? db.schemaRole.procedures : db.guestRole.procedures))
        { }
        protected Role(long defpos, BTree<long, object> m) : base(defpos, m) { }
        public static Role operator+(Role r,(long,object)x)
        {
            return (Role)r.New(r.mem + x);
        }
        public static Role operator +(Role r, ObInfo ob)
        {
            var m = r.mem + (ob.defpos, ob);
            if (ob.name != "")
                m += (DBObjects, r.dbobjects + (ob.name, ob.defpos));
            return new Role(r.defpos, m);
        }
        public static Role operator+(Role r,PProcedure pp)
        {
            var ps = r.procedures;
            var pa = ps[pp.name] ?? BTree<int, long>.Empty;
            return new Role(r.defpos,r.mem+(Procedures,ps+(pp.name,pa+(pp.arity,pp.ppos))));
        }
        public static Role operator +(Role r, Procedure p)
        {
            var ps = r.procedures;
            var pa = ps[p.name] ?? BTree<int, long>.Empty;
            return new Role(r.defpos, r.mem + (Procedures, ps + (p.name, pa + (p.arity, p.defpos))));
        }
        public static Role operator +(Role r, Domain dm)
        {
            return new Role(r.defpos, r.mem + (dm.defpos, dm));
        }
        public static Role operator -(Role r, ObInfo ob)
        {
            return new Role(r.defpos, r.mem - ob.defpos + (DBObjects, r.dbobjects - ob.name));
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" ObInfos:");
            var cm = '(';
            for (var b = mem.PositionAt(0)?.Next(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ','; sb.Append(b.value());
            }
            if (cm == ',') sb.Append(')');
            sb.Append(" Domains:"); cm = '(';
            for (var b=dbobjects.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ',';
                sb.Append(b.key()); sb.Append('='); sb.Append(Uid(b.value()));
            }
            if (cm == ',') sb.Append(')');
            sb.Append(" Procedures:"); cm = '(';
            for (var b=procedures.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ';';
                sb.Append(b.key()); sb.Append('=');
                var cn = '[';
                for (var a = b.value().First(); a != null; a = a.Next())
                {
                    sb.Append(a.key()); sb.Append(':');
                    sb.Append(cn); cn = ',';
                    sb.Append(Uid(a.value()));
                }
                if (cn == ',') sb.Append(']');
            }
            if (cm == ';') sb.Append(')');
            return sb.ToString();
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Role(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            throw new NotImplementedException();
        }
        internal override Basis Relocate(Writer wr)
        {
            throw new NotImplementedException();
        }
    }
    /// <summary>
    /// Immutable (everything in level 3 must be immutable)
    /// mem includes Selectors and some metadata
    /// </summary>
    internal class ObInfo : DBObject
    {
        internal const long
            Columns = -250, // BList<SqlValue> 
            Map = -251, //  BTree<string,int?> 
            Privilege = -253, // Grant.Privilege
            Properties = -254; // BTree<string,long> value object added into vacant mem above 0
                               // (not for bool or long)
        internal const string // some standard property names used in the code
            Attribute = "Attribute", // BTree<string,bool>
            Caption = "Caption", // string
            Csv = "Csv", // bool
            _Description = "Description", // string for role (DBOBject also has Description)
            Entity = "Entity", // bool
            Histogram = "Histogram", // bool
            Id = "Id", // long
            Iri = "Iri", // string
            Inverts = "Inverts", // BTree<long,long>
            Json = "Json", // bool
            Legend = "Legend", // string
            Line = "Line", // bool
            Monotonic = "Monotonic", // BTree<long,bool>
            Points = "Points", // bool
            Pie = "Pie", // bool
            X = "X", // long
            Y = "Y"; // long
        public string name => (string)mem[Name] ?? "";
        public BList<SqlValue> columns =>
            (BList<SqlValue>)mem[Columns] ?? BList<SqlValue>.Empty;
        public BTree<string, int?> map =>
            (BTree<string, int?>)mem[Map] ?? BTree<string, int?>.Empty;
        public Grant.Privilege priv => (Grant.Privilege)(mem[Privilege] ?? Grant.AllPrivileges);
        public BTree<string, long> properties =>
            (BTree<string, long>)mem[Properties] ?? BTree<string, long>.Empty;
        public int Length => (int)columns.Count;
        internal readonly static ObInfo Any = new ObInfo();
        ObInfo() : base(-1, BTree<long, object>.Empty) { }
        public ObInfo(long dp, string nm, Domain dt = null, Grant.Privilege pr = 0, BTree<long, object> m = null)
            : this(dp, (m ?? BTree<long, object>.Empty) + (Name, nm) + (Privilege, pr)
                  +(_Domain,dt??Domain.Content)) { }
        /// <summary>
        /// Allow construction of nameless ad-hoc Row types: see Transaction.GetDomain()
        /// </summary>
        /// <param name="cols"></param>
        public ObInfo(long lp, Domain dt, BList<SqlValue> cols)
            : this(lp, BTree<long, object>.Empty + (Map, _Map(cols)) + (Columns, cols)
                  + (_Domain,dt)) { }
        public ObInfo(long lp, CList<TableColumn> cols,Transaction tr)
            : this(lp, Domain.Row, _Cols(cols,tr)) { }
        protected ObInfo(long dp, BTree<long, object> m) : base(dp, m) { }
        public static ObInfo operator +(ObInfo oi, (long, object) x)
        {
            return new ObInfo(oi.defpos, oi.mem + x);
        }
        public static ObInfo operator +(ObInfo d, SqlValue s)
        {
            var iq = d.map[s.name];
            if (iq != null && d.columns[iq.Value].defpos == s.defpos)
                return d;
            var k = (int)d.columns.Count;
            d += (Columns, d.columns + s);
            if (s.name!=null)
                d += (Map, d.map + (s.name, k));
            return d;
        }
        public static ObInfo operator +(ObInfo d, (int,SqlValue) x)
        {
            d += (Columns, d.columns + x);
            return d;
        }
        public static ObInfo operator-(ObInfo d,SqlValue s)
        {
            var cs = BList<SqlValue>.Empty;
            var mp = BTree<string, int?>.Empty;
            for (var b = d.columns.First(); b != null; b = b.Next())
                if (s.CompareTo(b.value()) != 0)
                {
                    var c = b.value();
                    mp += (c.name, (int)cs.Count);
                    cs += c;
                }
            return d + (Columns, cs) + (Map, mp);
        }
        public static ObInfo operator +(ObInfo d, Metadata md)
        {
            var ps = d.properties;
            var mm = d.mem;
            if (md.description != "")
            {
                var pp = _Gap(d, 0);
                ps += (_Description, pp);
                mm += (pp, md.description);
            }
            if (md.iri != "")
            {
                var pp = _Gap(d, 0);
                ps += (Iri, pp);
                mm += (pp, md.iri);
            }
            mm += (Properties, ps);
            return (ObInfo)d.New(mm);
        }
        static long _Gap(Basis a, long off)
        {
            long r = off;
            while (a.mem.Contains(r))
                r++;
            return r;
        }
        static BTree<string, int?> _Map(BList<SqlValue> cols)
        {
            var r = BTree<string, int?>.Empty;
            var k = 0;
            for (var b = cols.First(); b != null; b = b.Next(),k++)
                if (b.value() is SqlValue v)
                    r += (v.name, k);
            return r;
        }
        static BList<SqlValue> _Cols(CList<TableColumn> cols,Transaction tr)
        {
            var r = BList<SqlValue>.Empty;
            for (var b = cols.First(); b != null; b = b.Next())
            {
                var tc = b.value();
                var oi = (ObInfo)tr.role.obinfos[tc.defpos];
                r += new SqlCol(tc.defpos,oi.name,tc);
            }
            return r;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ObInfo(defpos, m);
        }
        internal override DBObject Relocate(long dp)
        {
            return new ObInfo(dp, mem);
        }
        internal SqlValue ColFor(string s)
        {
            var iq = map[s];
            if (iq == null)
                return null;
            return columns[iq.Value];
        }
        internal ObInfo For(Database db, DBObject ob, Grant.Privilege pr)
        {
            var tr = db as Transaction;
            if (ob.Denied(tr, pr))
                throw new DBException("42105");
            var cs = columns;
            for (var b = cs.First(); b != null; b = b.Next())
                if (!b.value().Denied(tr, pr))
                    cs += (b.key(), b.value());
            if (cs == BList<SqlValue>.Empty)
                throw new DBException("42105");
            return (cs.Count == columns.Count) ? this : this + (Columns, cs);
        }
        public bool EqualOrStrongSubtypeOf(ObInfo oi)
        {
            var tb = oi.columns.First();
            for (var b = columns.First(); b != null && tb != null; b = b.Next(), tb = tb.Next())
                if (!b.value().domain.EqualOrStrongSubtypeOf(tb.value().domain))
                    return false;
            return true;
        }
        internal int Compare(TRow a,TRow b)
        {
            var ab = a.values.First();
            var bb = b.values.First();
            var c = 0;
            for (var db = columns.First(); c == 0 && db != null; db = db.Next(), ab = ab.Next(), bb = bb.Next())
                c = db.value().domain.Compare(ab.value(), bb.value());
            return c;
        }
        public override TypedValue Get(Reader rdr)
        {
            var r = BList<TypedValue>.Empty;
            for (var b = columns.First(); b != null; b = b.Next())
                r += b.value().Get(rdr);
            return new TRow(this, r);
        }
        public override void Put(TypedValue tv, Writer wr)
        {
            for (var b = columns.First(); b != null; b = b.Next())
            {
                var sc = b.value();
                sc.domain.Put(tv[sc.defpos],wr);
            }
        }
        public override TypedValue Parse(Scanner lx,bool union=false)
        {
            var start = lx.pos;
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
                    return ParseList(lx);
                int j = 0;
                var cols = new TypedValue[Length];
                for (lx.White(); lx.ch == comma; j++)
                {
                    lx.Advance();
                    lx.White();
                    var n = lx.GetName();
                    if (n != null) // column name supplied
                    {
                        if (lx.ch != ':')
                            throw new DBException("42124").Mix();
                        else
                            lx.Advance();
                        j = map[n] ??
                            throw new DBException("42124");
                    }
                    lx.White();
                    var co = columns[j];
                    var dt = co.mem.Contains(_Domain) ? co.domain : Domain.Content;
                    cols[j] = dt.Parse(lx);
                    comma = ',';
                    lx.White();
                }
                if (lx.ch != end)
                {
                    var xs = new string(lx.input, start, lx.pos - start);
                    throw new DBException("22005E", ToString(), xs).ISO()
                        .AddType(this).AddValue(new TChar(xs));
                }
                lx.Advance();
                return new TRow(this, cols);
            }
        }
        TypedValue ParseList(Scanner lx)
        {
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
        /// Coerce a given value to this type, bomb if it isn't possible
        /// </summary>
        /// <param name="v"></param>
        /// <returns></returns>
        internal override TypedValue Coerce(TypedValue v)
        {
            var vk = Domain.Equivalent(v.dataType.kind);
            if (vk == Sqlx.DOCUMENT)
            {
                var doc = v as TDocument;
                var obs = new TypedValue[Length];
                for (var b = columns.First(); b != null; b = b.Next())
                    obs[b.key()] = doc[b.value().name] ?? b.value().domain.defaultValue;
                return new TRow(this, obs);
            }
            throw new DBException("22005", this, v.ToString()).ISO();
        }
        internal override TypedValue Coerce(RowBookmark rb)
        {
            var q = rb._rs.qry;
            var v = rb.row;
            if (v.Length > q.display)
                v = new TRow(q.rowType, v.values);
            return Coerce(v);
        }
        internal override DBObject Replace(Context cx, DBObject so, DBObject sv)
        {
            var r = this;
            var cs = BList<SqlValue>.Empty;
            var ch = false;
            for (var b=columns.First();b!=null;b=b.Next())
            {
                var c = (SqlValue)b.value().Replace(cx, so, sv);
                if (c != b.value())
                    ch = true;
                cs += c;
            }
            if (ch)
                r += (Columns, cs);
            return r;
        }
        internal override Basis Relocate(Writer wr)
        {
            if (defpos < 0)
                return this;
            var r = this;
            var d = wr.Fix(defpos);
            if (d != defpos)
                r = (ObInfo)Relocate(d);
            var c = BList<SqlValue>.Empty;
            var ch = false;
            for (var b = columns?.First(); b != null; b = b.Next())
            {
                var s = (SqlValue)b.value().Relocate(wr);
                ch = ch || (b.value() != s);
                c += s;
            }
            if (ch)
                r += (Columns, c);
            return r;
        }
        /// <summary>
        /// Decide which type we want for a union type and an actual value
        /// </summary>
        /// <param name="v">the value</param>
        /// <returns>the type from the union appropriate for this value</returns>
        internal override DBObject TypeOf(long lp, Context cx, TypedValue v)
        {
            int n = (int)columns.Count;
            for (int j = 0; j < n; j++)
            {
                var r = columns[j].domain;
                if (r.CanTakeValueOf(v.dataType))
                    return r;
            }
            return null;
        }
        internal bool CanTakeValueOf(ObInfo oi)
        {
            if (Length != oi.Length)
                return false;
            var tb = oi.columns.First();
            for (var b = columns.First(); b != null; b = b.Next(), tb = tb.Next())
                if (b.value().defpos != tb.value().defpos)
                    return false;
            return true;
        }
        internal bool CanBeAssigned(ObInfo oi)
        {
            if (Length != oi.Length)
                return false;
            var tb = oi.columns.First();
            for (var b = columns.First(); b != null; b = b.Next(), tb = tb.Next())
                if (!b.value().domain.CanTakeValueOf(tb.value().domain))
                    return false;
            return true;
        }
        public string Props(Database db)
        {
            var sb = new StringBuilder();
            var cm = "";
            for (var b = properties.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = ",";
                switch (b.key())
                {
                    default: // boolean property
                        sb.Append(b.key());
                        break;
                    case Caption: // string property
                    case _Description:
                    case Iri:
                    case Legend:
                        sb.Append(b.key());
                        sb.Append("=");
                        sb.Append((string)mem[b.value()]);
                        break;
                    case Id: // long
                        sb.Append(Uid(b.value()));
                        break;
                    case Attribute:
                    case Monotonic:
                        {
                            sb.Append(b.key());
                            sb.Append("(");
                            var at = (BTree<long, bool>)mem[b.value()];
                            for (var a = at?.First(); a != null; a = a.Next())
                            {
                                var ai = (ObInfo)db.role.obinfos[a.key()];
                                sb.Append(ai?.name ?? "?");
                                sb.Append(' ');
                            }
                            sb.Append(")");
                            break;
                        }
                    case Inverts:
                        {
                            sb.Append("Inverses(");
                            var at = (BTree<long, long>)mem[b.value()];
                            for (var a = at?.First(); a != null; a = a.Next())
                            {
                                var ai = (ObInfo)db.role.obinfos[a.key()];
                                sb.Append(ai?.name ?? "?");
                                sb.Append(":");
                                var bi = (ObInfo)db.role.obinfos[a.value()];
                                sb.Append(bi?.name ?? "?");
                                sb.Append(' ');
                            }
                            sb.Append(")");
                            break;
                        }
                }
            }
            return sb.ToString();
        }
        internal string Xml(Transaction tr, Context cx, long dp,TRow r)
        {
            var sb = new StringBuilder();
            var ro = tr.role;
            var sc = sb;
            if (ro != null)
                sb.Append("<" + ro.name);
            var ss = new string[r.info.Length];
            for (int i = 0; i < r.info.Length; i++)
            {
                var tv = r[i];
                if (tv == null)
                    continue;
                var kn = r.info.columns[i].defpos;
                var p = tv.dataType;
                var m = (tr.objects[kn] as DBObject).Meta();
                if (tv != null && !tv.IsNull && m != null && m.Has(Sqlx.ATTRIBUTE))
                    sb.Append(" " + kn + "=\"" + tv.ToString() + "\"");
                else if (tv != null && !tv.IsNull)
                {
                    ss[i] = "<" + kn + " type=\"" + p.ToString() + "\">" +
                        p.Xml(tr, cx, defpos, tv) + "</" + kn + ">";
                }
            }
            return sb.ToString();
        }
        /// <summary>
        /// API development support: generate the C# type information for a field 
        /// </summary>
        /// <param name="dt">The type to use</param>
        /// <param name="db">The database</param>
        /// <param name="sb">a string builder</param>
        internal void DisplayType(Database db, StringBuilder sb)
        {
            for (var i = 0; i < columns.Count; i++)
            {
                var c = (SqlValue)columns[i];
                var cd = c.domain;
                var n = c.name.Replace('.', '_');
                var di = (ObInfo)db.role.obinfos[cd.defpos];
                var tn = di.name;
                if (cd.kind != Sqlx.TYPE && cd.kind != Sqlx.ARRAY && cd.kind != Sqlx.MULTISET)
                    tn = cd.SystemType.Name;
                if (cd.kind == Sqlx.ARRAY || cd.kind == Sqlx.MULTISET)
                {
                    if (tn == "[]")
                        tn = "_T" + i + "[]";
                    if (n.EndsWith("("))
                        n = "_F" + i;
                }
                FieldType(db, sb, cd);
                sb.Append("  public " + tn + " " + n + ";\r\n");
            }
            for (var i = 0; i < Length; i++)
            {
                var cd = columns[i].domain;
                if (cd.kind != Sqlx.ARRAY && cd.kind != Sqlx.MULTISET)
                    continue;
                cd = cd.elType.domain;
                var di = (ObInfo)db.role.obinfos[cd.defpos];
                var tn = di.name.ToString();
                if (tn != null)
                    sb.Append("// Delete this declaration of class " + tn + " if your app declares it somewhere else\r\n");
                else
                    tn += "_T" + i;
                sb.Append("  public class " + tn + " {\r\n");
                di.DisplayType(db, sb);
                sb.Append("  }\r\n");
            }
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" "); sb.Append(name); sb.Append(" ");
            var cm = "(";
            for (var b=columns.First();b!=null;b=b.Next())
            {
                sb.Append(cm); cm = ",";
                sb.Append(b.value());
            }
            if (cm == ",") sb.Append(") "); 
            if (mem.Contains(Privilege))
            {
                sb.Append(" Privilege="); sb.Append((long)priv);
            }
            if (mem.Contains(Properties))
            {
                sb.Append(" Properties: "); sb.Append(properties); 
                for (var b=mem.PositionAt(0);b!=null;b=b.Next())
                {
                    sb.Append(cm); cm = ",";
                    sb.Append(Uid(b.key())); sb.Append("="); sb.Append(b.value());
                }
            }
            return sb.ToString();
        }
    }
    internal class ObInfoOldRow : ObInfo
    {
        public ObInfoOldRow(ObInfo oi,long trs) : base(oi.defpos, _Old(oi,trs).mem) { }
        public ObInfoOldRow(Table tb, Role ro,long trs)
            : this((ObInfo)ro.obinfos[tb.defpos],trs) { }
        protected ObInfoOldRow(long dp, BTree<long, object> m) : base(dp, m) { }
        public static ObInfoOldRow operator+(ObInfoOldRow oi,(long,object)x)
        {
            return (ObInfoOldRow)oi.New(oi.mem + x);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ObInfoOldRow(defpos,m);
        }
        static ObInfo _Old(ObInfo oi,long trs)
        {
            var cs = BList<SqlValue>.Empty;
            for (var b = oi.columns.First(); b != null; b = b.Next())
            {
                var sc = (SqlCol)b.value();
                cs += new SqlOldRowCol(sc.defpos,trs,sc);
            }
            return oi+(Columns,cs);
        }
    }
}