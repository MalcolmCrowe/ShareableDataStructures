using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System;
using System.Net;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
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
        internal const long
            DBObjects = -248, // BTree<string,long> Domain/Table/View etc by name
            Procedures = -249; // BTree<string,BTree<int,long>> Procedure/Function by name and arity
        public string name => (string)(mem[Name] ?? "");
        internal BTree<string, long> dbobjects => 
            (BTree<string, long>)mem[DBObjects]??BTree<string,long>.Empty;
        internal BTree<string, BTree<int,long>> procedures => // not BList<long> !
            (BTree<string, BTree<int,long>>)mem[Procedures]??BTree<string,BTree<int,long>>.Empty;
        public BTree<long, object> infos => mem;
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
            var (p, ob) = x;
            if (p > 0 && !(ob is ObInfo))
                throw new PEException("PE917");
            return (Role)r.New(r.mem + x);
        }
        public static Role operator +(Role r, (ObInfo,bool) x)
        {
            var (ob, nm) = x;
            var m = r.mem + (ob.defpos, ob);
            if (ob.name != "" && nm)
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
            var pa = ps[p.name] ?? CTree<int, long>.Empty;
            return new Role(r.defpos, r.mem + (Procedures, ps + (p.name, pa + (p.arity, p.defpos))));
        }
        public static Role operator +(Role r, Method p)
        {
            var oi = (ObInfo)r.infos[p.udType.defpos];
            var ms = oi.methodInfos;
            var pa = ms[p.name] ?? CTree<int, long>.Empty;
            return new Role(r.defpos, 
                r.mem + (oi.defpos, oi+(ObInfo.MethodInfos,ms+(p.name, pa + (p.arity, p.defpos)))));
        }
        public static Role operator -(Role r, ObInfo ob)
        {
            return new Role(r.defpos, r.mem - ob.defpos + (DBObjects, r.dbobjects - ob.name));
        }
        internal override void Scan(Context cx)
        {
            throw new NotImplementedException();
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
        internal override Basis _Relocate(Writer wr)
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
            MethodInfos = -252, // CTree<string, CTree<int,long>> Method
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
        public Grant.Privilege priv => (Grant.Privilege)(mem[Privilege] ?? Grant.AllPrivileges);
        public BTree<string, long> properties =>
            (BTree<string, long>)mem[Properties] ?? BTree<string, long>.Empty;
        public CTree<string, CTree<int, long>> methodInfos =>
(CTree<string, CTree<int, long>>)mem[MethodInfos] ?? CTree<string, CTree<int, long>>.Empty;

        internal readonly static ObInfo Any = new ObInfo();
        ObInfo() : base(-1, BTree<long, object>.Empty) { }
        /// <summary>
        /// ObInfo for Table, TableColumn, Procedure etc have role-specific RowType in domains
        /// </summary>
        /// <param name="lp"></param>
        /// <param name="name"></param>
        /// <param name="rt"></param>
        /// <param name="m"></param>
        public ObInfo(long lp, string name, Domain rt, BTree<long, object> m = null)
            : this(lp, (m ?? BTree<long, object>.Empty) + (Name, name)
          +(_Domain,rt)) { }
        public ObInfo(long lp, string name, Context cx, CList<long> rt,
            BTree<long, object> m = null) 
            : this(lp, (m ?? BTree<long, object>.Empty) + (Name, name)
                + (_Domain, ((DBObject)cx.db.objects[lp]).domain+(Domain.RowType,rt)))
        { }
        protected ObInfo(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        public static ObInfo operator +(ObInfo oi, (long, object) x)
        {
            return new ObInfo(oi.defpos, oi.mem + x);
        }
        public static ObInfo operator +(ObInfo ut, (Method, string) m)
        {
            var ms = ut.methodInfos[m.Item2] ?? CTree<int, long>.Empty;
            ms += (m.Item1.arity, m.Item1.defpos);
            return new ObInfo(ut.defpos, ut.mem + (MethodInfos, ut.methodInfos + (m.Item2, ms))
                + (m.Item1.defpos, m.Item2));
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
        internal override Basis New(BTree<long, object> m)
        {
            return new ObInfo(defpos, m);
        }
        internal override TypedValue Eval(Context cx)
        {
            return domain.Coerce(cx,(cx.obs[defpos] as SqlValue)?.Eval(cx) ?? cx.values[defpos]);
        }
        internal override BTree<long, Register> AddIn(Context _cx, Cursor rb, BTree<long, Register> tg)
        {
            if (_cx.obs[defpos] is SqlValue sv)
                tg = sv.AddIn(_cx, rb, tg);
            else
                for (var b = domain.representation.First(); b != null; b = b.Next())
                {
                    var p = b.key();
                    var v = _cx.obs[p] as SqlValue;
                    tg = v?.AddIn(_cx, rb, tg);
                }
            return tg;
        }
        internal override BTree<long, Register> StartCounter(Context _cx, RowSet rs, BTree<long, Register> tg)
        {
            if (_cx.obs[defpos] is SqlValue sv)
                tg = sv.StartCounter(_cx, rs, tg);
            for (var b = domain.representation.First(); b != null; b = b.Next())
            {
                var p = b.key();
                var v = _cx.obs[p] as SqlValue;
                tg = v?.StartCounter(_cx, rs, tg);
            }
            return tg;
        }
        internal override DBObject Relocate(long dp)
        {
            return new ObInfo(dp, mem);
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
                                var ai = (ObInfo)db.role.infos[a.key()];
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
                                var ai = (ObInfo)db.role.infos[a.key()];
                                sb.Append(ai?.name ?? "?");
                                sb.Append(":");
                                var bi = (ObInfo)db.role.infos[a.value()];
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
        /// <summary>
        /// API development support: generate the C# type information for a field 
        /// </summary>
        /// <param name="dt">The type to use</param>
        /// <param name="db">The database</param>
        /// <param name="sb">a string builder</param>
        internal void DisplayType(Database db, StringBuilder sb)
        {
            var i = 0;
            for (var b=domain.representation.First();b!=null;b=b.Next(), i++)
            {
                var p = b.key();
                var cd = b.value();
                var ci = (ObInfo)db.role.infos[p];
                var n = ci.name;
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
                FieldType(db, sb, cd);
                sb.Append("  public " + tn + " " + n + ";\r\n");
            }
            for (var b=domain.representation.First();b!=null;b=b.Next(), i++)
            {
                var p = b.key();
                var cd = b.value();
                if (cd.kind != Sqlx.ARRAY && cd.kind != Sqlx.MULTISET)
                    continue;
                cd = cd.elType;
                var di = (ObInfo)db.role.infos[b.key()];
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
            sb.Append(" "); sb.Append(name); 
            sb.Append(" "); sb.Append(domain);
            if (mem.Contains(Privilege))
            {
                sb.Append(" Privilege="); sb.Append((long)priv);
            }
            var cm = "";
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
        internal override void Scan(Context cx)
        {
            cx.Scan(properties);
        }
        internal override Basis _Relocate(Writer wr)
        {
            var r = base._Relocate(wr);
            r += (Properties, wr.Fix(properties));
            return r;
        }
        internal override Basis Fix(Context cx)
        {
            var r = base.Fix(cx);
            r += (Properties, cx.Fix(properties));
            return r;
        }
    }
}