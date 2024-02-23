using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using Pyrrho.Level5;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level3
{
    /// <summary>
    /// Level 3 Role object. 
    /// Roles manage grantable and renamable role-based database objects: 
    /// roles/users, table/views, procedures, domains/UDTypes.
    /// These also have infos for giving names/metadata and rowTypes of these objects.
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
    /// subject to normal SQL statements. The schema role has the same uid as _system.role,
    /// and maintains a tree of all users and roles known to the database.
    /// The guest role has access to all such that have been granted to PUBLIC: there is no Role object for this.
    /// In Pyrrho users are granted roles, and roles can be granted objects.  
    /// There is one exception: a single user can be granted ownership of the database:
    /// this happens by default for the first user to be granted the schema role.
    /// If a role is granted to another role it causes a cascade of permissions, but does not establish
    /// an ongoing relationship between the roles (granting a role to a user does):
    /// for this reason granting a role to a role is deprecated.
    /// Insert label sets (consisting of &-separated type names in canonical order) 
    /// are tolerated as keys in dbobjects. These compound names are constructed 
    /// in SqlNode._NodeType from CTree(long,bool) where long is an existing graph type, and in that case
    /// (i.e. if the name has &) we always get a NodeType, which, if we are expecting an Edge Type,
    /// will have the EdgeType we want as a subtype.
    /// Immutable
    /// 
    /// </summary>
    internal class Role : DBObject
    {
        internal const long
            DBObjects = -248, // BTree<string,long?> Domain/Table/View etc by name
            Procedures = -249; // BTree<string,BTree<CList<Domain>,long?>> Procedure/Function by name and arity
       internal BTree<string, long?> dbobjects => 
            (BTree<string, long?>?)mem[DBObjects]??BTree<string,long?>.Empty;
        public new string? name => (string?)mem[ObInfo.Name];
        internal BTree<string, BTree<CList<Domain>,long?>> procedures => 
            (BTree<string, BTree<CList<Domain>,long?>>?)mem[Procedures]??BTree<string, BTree<CList<Domain>, long?>>.Empty;
 
        public const Grant.Privilege use = Grant.Privilege.UseRole,
            admin = Grant.Privilege.UseRole | Grant.Privilege.AdminRole;
        /// <summary>
        /// Just to create the schema and guest roles
        /// </summary>
        /// <param name="nm"></param>
        /// <param name="u"></param>
        internal Role(string nm, long defpos, BTree<long, object> m)
            : base(defpos, defpos, (m ?? BTree<long, object>.Empty) + (ObInfo.Name, nm))
        { }
        public Role(PRole p, Database db, bool first)
            : base(p.ppos, p.ppos, _Mem(p,db,first))
        { }
        protected Role(long defpos, BTree<long, object> m) : base(defpos, m) { }
        static BTree<long, object> _Mem(PRole p, Database db, bool first)
        {
            if (db.role is not Role ro || db.guest is not Role gu)
                throw new DBException("42105");
            return ((first ? ro : gu).mem ?? BTree<long, object>.Empty) + (LastChange, p.ppos) 
                + (ObInfo.Name, p.name) + (Definer, p.definer) + (Infos,p.infos) + (Owner,p.owner)
            + (DBObjects, first ? db.schema.dbobjects : db.guest.dbobjects)
            + (Procedures, first ? db.schema.procedures : db.guest.procedures)
            + (Infos, p.infos);
        }
        public static Role operator+(Role r,(long,object)x)
        {
            var (dp, ob) = x;
            if (r.mem[dp] == ob)
                return r;
            return (Role)r.New(r.mem + x);
        }
        public static Role operator +(Role r, (string, long) x)
        {
            return (Role)r.New(r.mem + (DBObjects, r.dbobjects + x));
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" ObInfos:");
            var cm = '(';
            for (var b = dbobjects.First(); b != null; b = b.Next())
                if (b.value() is long p)
                {
                    sb.Append(cm); cm = ','; sb.Append(b.key()); sb.Append('=');
                    sb.Append(Uid(p));
                }
            if (cm == ',') sb.Append(')');
            sb.Append(" Procedures:"); cm = '(';
            for (var b=procedures.First();b is not null;b=b.Next())
            {
                sb.Append(cm); cm = ';';
                sb.Append(b.key()); sb.Append('=');
                var cn = '[';
                for (var a = b.value().First(); a != null; a = a.Next())
                    if (a.value() is long p)
                    {
                        sb.Append(a.key()); sb.Append(':');
                        sb.Append(cn); cn = ',';
                        sb.Append(Uid(p));
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
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new Role(dp, m);
        }
    }
    /// <summary>
    /// Immutable (everything in level 3 must be immutable)
    /// mem includes Selectors and some metadata.
    /// Names information works for UDType hierarchies as follows:
    /// The Names property for a UDType (always computed on-demand)
    /// is obtained by recursing through the obInfos for all types in the hierachy
    /// with names info for a type taking precedence over supertypes.
    /// On Select, corresponding data is then collected from the types and its supertypes.
    /// The integer component gives the column position of the name in the table that defines it.
    /// </summary>
    internal class ObInfo : Basis
    {
        internal const long
            _Metadata = -254, // CTree<Sqlx,TypedValue>
            Description = -67, // string
            Inverts = -353, // long Procedure
            MethodInfos = -252, // BTree<string, BTree<CList<Domain>,long?>> Method
            Name = -50, // string
            Names = -282, // BTree<string,(int,long?)> TableColumn (SqlValues in RowSet)
            SchemaKey = -286, // long (highwatermark for schema changes)
            Privilege = -253; // Grant.Privilege
        public string description => mem[Description]?.ToString() ?? "";
        public Grant.Privilege priv => (Grant.Privilege?)mem[Privilege]??Grant.Privilege.NoPrivilege;
        public long inverts => (long)(mem[Inverts] ?? -1L);
        public BTree<string, BTree<CList<Domain>, long?>> methodInfos =>
            (BTree<string, BTree<CList<Domain>, long?>>?)mem[MethodInfos] 
            ?? BTree<string, BTree<CList<Domain>, long?>>.Empty;
        public CTree<Sqlx, TypedValue> metadata =>
            (CTree<Sqlx, TypedValue>?)mem[_Metadata] ?? CTree<Sqlx, TypedValue>.Empty;
        public string? name => (string?)mem[Name] ?? "";
        internal BTree<string,(int,long?)> names =>
            (BTree<string, (int,long?)>?)mem[Names]??BTree<string,(int, long?)>.Empty;
        internal long schemaKey => (long)(mem[SchemaKey] ?? -1L);
        /// <summary>
        /// ObInfo for Table, TableColumn, Procedure etc have role-specific RowType in domains
        /// </summary>
        /// <param name="lp"></param>
        /// <param name="name"></param>
        /// <param name="rt"></param>
        /// <param name="m"></param>
        public ObInfo(string name, Grant.Privilege pr=0)
            : base(new BTree<long, object>(Name, name) + (Privilege, pr)) 
        { }
        protected ObInfo(BTree<long, object> m) : base(m)
        { }
        public static ObInfo operator +(ObInfo oi, (long, object) x)
        {
            var (dp, ob) = x;
            if (oi.mem[dp] == ob)
                return oi;
            return new ObInfo(oi.mem + x);
        }
        public static ObInfo operator-(ObInfo oi,long p)
        {
            return new ObInfo(oi.mem - p);
        }
        public static ObInfo operator +(ObInfo d, PMetadata pm)
        {
            d += (_Metadata, pm.Metadata());
            if (pm.detail[Sqlx.DESCRIBE] is TypedValue tv)
                d += (Description, tv);
            if (pm.refpos > 0)
                d += (Inverts, pm.refpos);
            return d;
        }
        public static ObInfo operator +(ObInfo d, ObInfo s)
        {
            return d + (Names, d.names + s.names);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ObInfo(m);
        }
        internal static string Metadata(CTree<Sqlx,TypedValue> md,string description="")
        {
            var sb = new StringBuilder();
            var cm = "";
            for (var b = md.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = " "; 
                switch (b.key())
                {
                    case Sqlx.URL:
                    case Sqlx.MIME:
                    case Sqlx.SQLAGENT:
                        sb.Append(b.key());
                        sb.Append(' '); sb.Append(b.value().ToString());
                        continue;
                    case Sqlx.PASSWORD:
                        sb.Append(b.key());
                        sb.Append(" ******");
                        continue;
                    case Sqlx.IRI:
                        sb.Append(b.key());
                        sb.Append(' '); sb.Append(b.value().ToString());
                        continue;
                    case Sqlx.NO: 
                        if (description != "")
                        { sb.Append(' '); sb.Append(description); }
                        continue;
                    case Sqlx.INVERTS:
                        sb.Append(b.key());
                        sb.Append(" "); sb.Append(DBObject.Uid(b.value().ToLong()??-1L));
                        continue;
                    case Sqlx.MIN:
                    case Sqlx.MAX:
                        {
                            var hi = md[Sqlx.MAX]?.ToInt();
                            if (hi is not null && b.key() == Sqlx.MIN)
                                continue; // already displayed
                            sb.Append("CARDINALITY(");
                            var lw = md[Sqlx.MIN]?.ToInt();
                            sb.Append(lw);
                            if (hi is not null)
                            {
                                sb.Append(" TO ");
                                sb.Append(hi);
                            }
                            sb.Append(')');
                        }
                        continue;
                    case Sqlx.MINVALUE:
                    case Sqlx.MAXVALUE:
                        {
                            var hi = md[Sqlx.MAXVALUE]?.ToInt();
                            if (hi is not null && b.key() == Sqlx.MINVALUE)
                                continue; // already displayed
                            sb.Append("MULTIPLICITY(");
                            var lw = md[Sqlx.MINVALUE]?.ToInt();
                            sb.Append(lw);
                            if (hi is not null)
                            {
                                sb.Append(" TO ");
                                sb.Append(hi);
                            }
                            sb.Append(')');
                        }
                        continue;
                    default:
                        sb.Append(b.key());
                        continue;
                }
            }
            return sb.ToString();
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" "); sb.Append(name);
            if (mem.Contains(Privilege))
            {
                sb.Append(" Privilege="); sb.Append((long)priv);
            }
            if (mem.Contains(_Metadata))
            { 
                sb.Append(" "); sb.Append(Metadata(metadata,description)); 
            }
            if (mem.Contains(Description))
            {
                sb.Append(' '); sb.Append(description);
            }
            return sb.ToString();
        }
        protected override BTree<long, object> _Fix(Context cx, BTree<long, object>m)
        {
            var r = base._Fix(cx,m);
            var ni = cx.Fix(inverts);
            if (ni!=inverts)
                r += (Inverts, ni);
            var ns = BTree<string, (int,long?)>.Empty;
            var ch = false;
            for (var b = names.First(); b != null; b = b.Next())
                if (b.value().Item2 is long p)
                {
                    p = cx.Fix(p);
                    if (p != b.value().Item2)
                        ch = true;
                    ns += (b.key(), (b.value().Item1, p));
                }
            if (ch)
                r += (Names, ns);
            return r;
        }
        internal override Basis ShallowReplace(Context cx, long was, long now)
        {
            var r = this;
            var md = ShallowReplace(cx, metadata, was, now);
            if (md != metadata)
                r += (_Metadata, md);
            var ns = ShallowReplace(cx, names, was, now);
            if (ns != names)
                r += (Names, ns);
            return r;
        }
        static CTree<Sqlx,TypedValue> ShallowReplace(Context cx,CTree<Sqlx,TypedValue> md,long was, long now)
        {
            for (var b=md.First();b!=null;b=b.Next())
                if (b.value() is TypedValue v)
                {
                    var nv = v.ShallowReplace(cx, was, now);
                    if (nv != v)
                        md += (b.key(), nv);
                }
            return md;
        }
        static BTree<string,(int,long?)>ShallowReplace(Context cx,BTree<string,(int,long?)> ns, long was, long now)
        {
            for (var b = ns.First(); b != null; b = b.Next())
            {
                var (i, p) = b.value();
                if (p!=null && p.Value==was)
                    ns += (b.key(), (i,now));
            }
            return ns;
        }
    }
}