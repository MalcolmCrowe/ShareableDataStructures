using System.ComponentModel;
using System.Text;
using System.Xml.Linq;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using Pyrrho.Level5;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
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
    /// The rowType role has access to system tables, and is the initial role for a new database.
    /// The database owner is always allowed to use the rowType role, but it is otherwise
    /// subject to normal SQL statements. The rowType role has the same uid as _system.role,
    /// and maintains a tree of all users and roles known to the database.
    /// The guest role has access to all such that have been granted to PUBLIC: there is no Role object for this.
    /// In Pyrrho users are granted roles, and roles can be granted objects.  
    /// There is one exception: a single user can be granted ownership of the database:
    /// this happens by default for the first user to be granted the rowType role.
    /// If a role is granted to another role it causes a cascade of permissions, but does not establish
    /// an ongoing relationship between the roles (granting a role to a user does):
    /// for this reason granting a role to a role is deprecated.
    /// Insert label sets (consisting of &-separated type names in canonical order) 
    /// are tolerated as keys in dbobjects. These compound names are constructed 
    /// in GqlNode._NodeType from CTree(long,bool) where long is an existing graph type, and in that case
    /// (i.e. if the name has &) we always get a NodeType, which, if we are expecting an Edge Type,
    /// will have the EdgeType we want as a subtype.
    /// We need extra lookup tables for NodeTypes and EdgeTypes because it is legal in GQL to have ambiguous names.
    /// For SQL purposes we can diambiguate where necessary with the defining position of a type as a numerical ID.
    /// Immutable
    /// 
    /// </summary>
    internal class Role : DBObject
    {
        internal const long
            DBObjects = -248, // BTree<string,long?> Domain/Table/View etc by name
            EdgeTypes = -128, // BTree<string,long?>> Labelled EdgeType/Result by name
            Graphs = -357,    // BTree<string,long?> Labelled Graph by name
            NodeTypes = -115, // BTree<string,long?> Labelled NodeType by name
            PropertyNames = -243, // CTree<string,CTree<long,bool>> Domains by child names
            Procedures = -249, // BTree<string,BTree<CList<Domain>,long?>> Procedure/Function by name and arity
            Schemas = -356,    // BTree<string,long?> RowType by name
            UnlabelledNodeTypesInfo = -476, //BTree<CTree<string,bool>,long?> NodeType by properties
            UnlabelledEdgeTypesInfo = -482; //BTree<CTree<string,bool>,long?> EdgeType by properties
        internal BTree<string, long?> dbobjects => 
            (BTree<string, long?>?)mem[DBObjects]??BTree<string,long?>.Empty;
        public new string? name => (string?)mem[ObInfo.Name];
        internal BTree<string, BTree<CList<Domain>,long?>> procedures => 
            (BTree<string, BTree<CList<Domain>,long?>>?)mem[Procedures]??BTree<string, BTree<CList<Domain>, long?>>.Empty;
        internal BTree<string,long?> nodeTypes =>
            (BTree<string, long?>)(mem[NodeTypes]??BTree<string,long?>.Empty);
        /// <summary>
        /// edgeTypes[name] gives the defining position of single EdgeType or a UNION of edgetypes
        /// </summary>
        internal BTree<string, long?> edgeTypes =>
            (BTree<string, long?>)(mem[EdgeTypes] ?? BTree<string, long?>.Empty);
        internal BTree<CTree<string, bool>, long?> unlabelledNodeTypesInfo =>
            (BTree<CTree<string, bool>, long?>)(mem[UnlabelledNodeTypesInfo]
            ?? BTree<CTree<string, bool>, long?>.Empty);
        internal BTree<CTree<string, bool>, long?> unlabelledEdgeTypesInfo =>
            (BTree<CTree<string, bool>, long?>)(mem[UnlabelledEdgeTypesInfo]
            ?? BTree<CTree<string, bool>, long?>.Empty);
        internal CTree<string, CTree<long, bool>> propertyNames =>
            (CTree<string, CTree<long, bool>>)(mem[PropertyNames] 
            ?? CTree<string, CTree<long, bool>>.Empty);
        internal BTree<string, long?> graphs =>
            (BTree<string, long?>)(mem[Graphs] ?? BTree<string, long?>.Empty);
        internal BTree<string, long?> schemas =>
            (BTree<string, long?>)(mem[Schemas] ?? BTree<string, long?>.Empty); 
        public const Grant.Privilege use = Grant.Privilege.UseRole,
            admin = Grant.Privilege.UseRole | Grant.Privilege.AdminRole;
        internal long home_graph => (long)(mem[Executable.UseGraph] ?? -1);
        /// <summary>
        /// Just to create the rowType and guest roles
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
                throw new DBException("42105").Add(Qlx.USER);
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
            if (procedures.Count > 0)
            {
                sb.Append(" Procedures:"); cm = '(';
                for (var b = procedures.First(); b is not null; b = b.Next())
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
            }
            if (schemas.Count > 0)
            {
                sb.Append(" Schemas:");
                cm = '(';
                for (var b = schemas.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = ','; sb.Append(b.key()); sb.Append('=');
                        sb.Append(Uid(p));
                    }
                if (cm == ',') sb.Append(')');
            }
            if (graphs.Count > 0)
            {
                sb.Append(" Graphs:");
                cm = '(';
                for (var b = graphs.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = ','; sb.Append(b.key()); sb.Append('=');
                        sb.Append(Uid(p));
                    }
                if (cm == ',') sb.Append(')');
            }
            if (nodeTypes.Count > 0)
            {
                sb.Append(" NodeTypes:");
                cm = '(';
                for (var b = nodeTypes.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = ','; sb.Append(b.key()); sb.Append('=');
                        sb.Append(Uid(p));
                    }
                if (cm == ',') sb.Append(')');
            }
            if (edgeTypes.Count > 0)
            {
                sb.Append(" EdgeTypes:");
                cm = '(';
                for (var b = edgeTypes.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                    {
                        sb.Append(cm); cm = ',';
                        sb.Append(b.key()); sb.Append(Uid(p));
                    }
                if (cm != '(') sb.Append(')');
            }
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
            _Metadata = -254, // CTree<Qlx,TypedValue>
            Defs = -367,        // BTree<long,Names> DBObject  
            Description = -67, // string
            Inverts = -353, // long Procedure
            MethodInfos = -252, // BTree<string, BTree<CList<Domain>,long?>> Method
            Name = -50, // string
            _Names = -282, // Names TableColumn (SqlValues in RowSet)
            SchemaKey = -286, // long (highwatermark for rowType changes)
            Privilege = -253; // Grant.Privilege
        public string description => mem[Description]?.ToString() ?? "";
        public Grant.Privilege priv => (Grant.Privilege?)mem[Privilege]??Grant.Privilege.NoPrivilege;
        public long inverts => (long)(mem[Inverts] ?? -1L);
        public BTree<string, BTree<CList<Domain>, long?>> methodInfos =>
            (BTree<string, BTree<CList<Domain>, long?>>?)mem[MethodInfos] 
            ?? BTree<string, BTree<CList<Domain>, long?>>.Empty;
        public TMetadata metadata => (TMetadata?)mem[_Metadata] ?? TMetadata.Empty;
        public string? name => (string?)mem[Name] ?? "";
        internal Names names =>
            (Names)(mem[_Names]??Names.Empty);
        internal BTree<long,Names> defs =>
                        (BTree<long, Names>)(mem[Defs] ?? BTree<long, Names>.Empty);
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
            if (pm.detail[Qlx.DESCRIBE] is TypedValue tv)
                d += (Description, tv);
            if (pm.refpos > 0)
                d += (Inverts, pm.refpos);
            return d;
        }
        public static ObInfo operator +(ObInfo d, ObInfo s)
        {
            return d + (_Names, d.names + s.names);
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ObInfo(m);
        }
        internal static string Metadata(TMetadata md,string description="")
        {
            var sb = new StringBuilder();
            var cm = "";
            for (var b = md.First(); b != null; b = b.Next())
            {
                sb.Append(cm); cm = " "; 
                switch (b.key())
                {
                    case Qlx.URL:
                    case Qlx.MIME:
                    case Qlx.SQLAGENT:
                        sb.Append(b.key());
                        sb.Append(' '); sb.Append(b.value().ToString());
                        continue;
                    case Qlx.PASSWORD:
                        sb.Append(b.key());
                        sb.Append(" ******");
                        continue;
                    case Qlx.IRI:
                        sb.Append(b.key());
                        sb.Append(' '); sb.Append(b.value().ToString());
                        continue;
                    case Qlx.NO: 
                        if (description != "")
                        { sb.Append(' '); sb.Append(description); }
                        continue;
                    case Qlx.INVERTS:
                        sb.Append(b.key());
                        sb.Append(' '); sb.Append(DBObject.Uid(b.value().ToLong()??-1L));
                        continue;
                    case Qlx.MIN:
                    case Qlx.MAX:
                        {
                            var hi = md[Qlx.MAX]?.ToInt();
                            if (hi is not null && b.key() == Qlx.MIN)
                                continue; // already displayed
                            sb.Append("CARDINALITY(");
                            var lw = md[Qlx.MIN]?.ToInt();
                            sb.Append(lw);
                            if (hi is not null)
                            {
                                sb.Append(" TO ");
                                sb.Append(hi);
                            }
                            sb.Append(')');
                        }
                        continue;
                    case Qlx.MINVALUE:
                    case Qlx.MAXVALUE:
                        {
                            var hi = md[Qlx.MAXVALUE]?.ToInt();
                            if (hi is not null && b.key() == Qlx.MINVALUE)
                                continue; // already displayed
                            sb.Append("MULTIPLICITY(");
                            var lw = md[Qlx.MINVALUE]?.ToInt();
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
            sb.Append(' ') ; sb.Append(name);
            if (mem.Contains(Privilege))
            {
                sb.Append(" Privilege="); sb.Append((long)priv);
            }
            if (mem.Contains(_Metadata))
            { 
                sb.Append(' '); sb.Append(Metadata(metadata,description)); 
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
            var ns = Names.Empty;
            var ch = false;
            for (var b = names.First(); b != null; b = b.Next())
                if (b.value().Item2 is long p)
                {
                    p = cx.Fix(p);
                    if (p != b.value().Item2)
                        ch = true;
                    ns += (b.key(), (b.value().Item1,p));
                }
            if (ch)
                r += (_Names, ns);
            return r;
        }
        internal ObInfo _Fix(Context cx)
        {
            return new ObInfo(_Fix(cx, mem));
        }
        internal override Basis ShallowReplace(Context cx, long was, long now)
        {
            var r = this;
            var md = ShallowReplace(cx, metadata, was, now);
            if (md != metadata)
                r += (_Metadata, md);
            var ns = ShallowReplace(cx, names, was, now);
            if (ns != names)
                r += (_Names, ns);
            return r;
        }
        static TMetadata ShallowReplace(Context cx,TMetadata md,long was, long now)
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
        static Names ShallowReplace(Context cx,Names ns, long was, long now)
        {
            for (var b = ns.First(); b != null; b = b.Next())
            {
                if (b.value().Item2 ==was)
                    ns += (b.key(),(b.value().Item1,now));
            }
            return ns;
        }
    }
}