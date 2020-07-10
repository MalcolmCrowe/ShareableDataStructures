using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
using System;
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
            Procedures = -249, // BTree<string,BTree<int,long>> Procedure/Function by name and arity
            Signatures = -52; // BTree<RowType,long> TableColumn, Table
        public string name => (string)(mem[Name] ?? "");
        internal BTree<string, long> dbobjects => 
            (BTree<string, long>)mem[DBObjects]??BTree<string,long>.Empty;
        internal BTree<string, BTree<int,long>> procedures => // not BList<long> !
            (BTree<string, BTree<int,long>>)mem[Procedures]??BTree<string,BTree<int,long>>.Empty;
        public BTree<long, object> infos => mem;
        public const Grant.Privilege use = Grant.Privilege.UseRole,
            admin = Grant.Privilege.UseRole | Grant.Privilege.AdminRole;
        public BTree<RowType, long> signatures =>
            (BTree<RowType, long>)mem[Signatures] ?? BTree<RowType, long>.Empty;
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
        public static Role operator +(Role r, Procedure p)
        {
            r += new ObInfo(p.defpos, p.name, Sqlx.PROCEDURE, p.domain, p.domain.structure);
            var ps = r.procedures;
            var pa = ps[p.name] ?? BTree<int, long>.Empty;
            return new Role(r.defpos, r.mem + 
                (Procedures, ps + (p.name, pa + (p.arity, p.defpos))));
        }
        public static Role operator +(Role r, PMethod p)
        {
            var ps = r.procedures;
            var pa = ps[p.name] ?? BTree<int, long>.Empty;
            return new Role(r.defpos, r.mem + (Procedures, ps + (p.name, pa + (p.arity, p.defpos))));
        }
        public static Role operator -(Role r, ObInfo ob)
        {
            return new Role(r.defpos, r.mem - ob.defpos + (DBObjects, r.dbobjects - ob.name));
        }
        internal override Sqlx kind => Sqlx.ROLE;
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
        internal override Basis _Relocate(Context cx)
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
            Methods = -252, // BTree<string, BTree<int,long>> Method
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
        internal override Sqlx kind => (Sqlx)(mem[Domain.Prim] ?? Sqlx.NONE);
        public Grant.Privilege priv => (Grant.Privilege)(mem[Privilege] ?? Grant.AllPrivileges);
        public BTree<string, long> properties =>
            (BTree<string, long>)mem[Properties] ?? BTree<string, long>.Empty;
        /// <summary>
        /// The set of Methods for this Type
        /// </summary>
        public BTree<string, BTree<int, long>> methods =>
            (BTree<string, BTree<int, long>>)mem[Methods] ?? BTree<string, BTree<int, long>>.Empty;
        public int Length => rowType.Length;
        internal readonly static ObInfo Any = new ObInfo();
        ObInfo() : base(-1, BTree<long, object>.Empty) { }
        public ObInfo(long lp, Domain dm, RowType cols,Sqlx k=Sqlx.ROW, BTree<long,object>m=null)
            : this(lp, (m??BTree<long, object>.Empty) + (_Domain,dm)+(_RowType, cols)
                  + (Domain.Prim,k)) { }
        public ObInfo(long lp, string name, Sqlx k=Sqlx.COLUMN, Domain dt=null, 
            RowType cols=null)
            : this(lp, BTree<long, object>.Empty + (Name, name) +(Domain.Prim,k)
                  + (_Domain, dt??Domain.Null) + (_RowType, cols??RowType.Empty)) { }
        public ObInfo(long lp, Context cx, BList<SqlValue> vs)
            : this(lp, _Dom(-1, cx, Domain.Row, vs) + (Privilege, Grant.AllPrivileges)
                  +(Domain.Prim,Sqlx.ROW)) { }
        protected ObInfo(long dp, BTree<long, object> m) : base(dp, m) 
        { }
        public static ObInfo operator +(ObInfo oi, (long, object) x)
        {
            return new ObInfo(oi.defpos, oi.mem + x);
        }
        public static ObInfo operator +(ObInfo ut, (Method, string) m)
        {
            var ms = ut.methods[m.Item2] ?? BTree<int, long>.Empty;
            ms += (m.Item1.arity, m.Item1.defpos);
            return new ObInfo(ut.defpos, ut.mem + (Methods, ut.methods + (m.Item2, ms))
                + (m.Item1.defpos, m.Item2));
        }
        public static ObInfo operator+(ObInfo oi,(int,long,Domain)x)
        {
            var (i, p, d) = x;
            return new ObInfo(oi.defpos, oi.mem + (_RowType, oi.rowType + (i, (p,d)))
                + (_Domain, oi.domain + (p, d)));
        }
        public static ObInfo operator -(ObInfo oi, long p)
        {
            var cs = RowType.Empty;
            for (var b=oi.rowType?.First();b!=null;b=b.Next())
            {
                var bp = b.value();
                if (p != bp.Item1)
                    cs += bp;
            }
            return new ObInfo(oi.defpos, oi.mem + (_Domain, oi.domain - p) + (_RowType, cs));
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
        internal long this[int i] => rowType[i].Item1;
        static long _Gap(Basis a, long off)
        {
            long r = off;
            while (a.mem.Contains(r))
                r++;
            return r;
        }
        static BTree<long,object> _Dom(long dp,Context cx,Domain dt, BList<SqlValue> vs)
        {
            var cs = RowType.Empty;
            var rs = BTree<long, Domain>.Empty;
            for (var b = vs.First(); b != null; b = b.Next())
            {
                var v = b.value();
                cs += (v.defpos, v.domain);
                rs += (v.defpos, v.domain);
            }
            var dm = new Domain(dp, dt, rs);
            return BTree<long,object>.Empty + (_Domain, dm) + (_RowType, cs);
        }
        static Domain _Dom(long dp, Context cx, ObInfo oi, BList<long> cs)
        {
            var rs = BTree<long, Domain>.Empty;
            var ob = oi.rowType?.First();
            var b = cs.First();
            for (; b != null & ob!=null; b = b.Next(),ob=ob.Next())
            {
                var cp = b.value();
                var op = ob.value().Item1;
                rs += (cp, oi.domain.representation[op]);
            }
            if (b != null || ob != null)
                throw new PEException("PE945");
            var dm = new Domain(dp, Domain.For(oi.domain.prim), rs);
            cx.Add(dm);
            return dm;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new ObInfo(defpos, m);
        }
        internal override TypedValue Eval(Context cx)
        {
            return domain.Coerce(cx,(cx.obs[defpos] as SqlValue)?.Eval(cx) ?? cx.values[defpos]);
        }
        internal PRow MakeKey(Context cx)
        {
            PRow k = null;
            for (var b = rowType?.Last(); b != null; b = b.Previous())
                k = new PRow(cx.obs[b.value().Item1].Eval(cx), k);
            return k;
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
        internal override TypedValue Coerce(Context cx,TypedValue v)
        {
            var vs = BTree<long, TypedValue>.Empty;
            if (Length==0)
                return domain.Coerce(cx,v);
            if (v is TRow rw)
            {
                var rb = rw.columns.First();
                for (var b = rowType?.First(); b != null; b = b.Next(), rb = rb.Next())
                {
                    if (rb == null)
                        goto bad;
                    var cp = b.value().Item1;
                    var rp = rb.value().Item1;
                    if (rw.values[rp] is TypedValue rv)
                        vs += (cp, domain.representation[cp].Coerce(cx,rv));
                    else
                        break;
                }
                if (rb== null)
                    return new TRow(rowType, domain, vs);
            }
            else if (Length == 1)
                return domain.representation[rowType[0].Item1].Coerce(cx,v);
            bad:;
            throw new DBException("22005", this, v.ToString()).ISO();
        }
        internal int PosFor(Context cx,string nm)
        {
            var i = 0;
            for (var b = rowType?.First(); b != null; b = b.Next(),i++)
                if (((ObInfo)cx.role.infos[b.value().Item1]).name == nm)
                    return i;
            return -1;
        }
        internal override DBObject Relocate(long dp)
        {
            return new ObInfo(dp, mem);
        }
        internal override DBObject _Replace(Context cx, DBObject so, DBObject sv)
        {
            // NB we can't use cx.done for Domains or ObInfos
            var r = this;
            var dm = domain.Replace(cx, so, sv);
            if (dm!=domain)
                r += (_Domain,dm);
            var cs = RowType.Empty;
            var ch = false;
            for (var b=r.rowType?.First();b!=null;b=b.Next())
            {
                var (p,d) = b.value();
                var nd = (Domain)d.Replace(cx, so, sv);
                var np = cx.Replace(p, so, sv);
                ch = ch || p != np || d != nd;
                cs += (np,nd);
            }
            if (ch)
                r += (_RowType, cs);
            // NB we can't use cx.done for Domains or ObInfos
            return r;
        }
        internal override Basis _Relocate(Writer wr)
        {
            var r = (ObInfo)base._Relocate(wr);
            var cs = RowType.Empty;
            var ch = false;
            for (var b = rowType?.First(); b != null; b = b.Next())
            {
                var c = b.value();
                var nc = wr.Fix(c);
                cs += nc;
                if (c != nc)
                    ch = true;
            }
            if (ch)
                r += (_RowType, cs);
            var dm = domain._Relocate(wr);
            if (dm != domain)
                r += (_Domain, dm);
            return r;
        }
        internal override Basis _Relocate(Context cx)
        {
            var r = (ObInfo)base._Relocate(cx);
            var cs = RowType.Empty;
            var ch = false;
            for (var b = rowType?.First(); b != null; b = b.Next())
            {
                var (c,d) = b.value();
                var nd = (Domain)d.Relocate(cx);
                var nc = cx.Unheap(c);
                cs += (nc,nd);
                if (c != nc || d!=nd)
                    ch = true;
            }
            if (ch)
                r += (_RowType, cs);
            var dm = domain._Relocate(cx);
            if (dm != domain)
                r += (_Domain, dm);
            return r;
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
        internal string Xml(Transaction tr, Context cx, long dp, TRow r)
        {
            var sb = new StringBuilder();
            var ro = tr.role;
            var sc = sb;
            if (ro != null)
                sb.Append("<" + ro.name);
            var ss = new string[r.Length];
            var i = 0;
            for (var b=cx.Dom(dp).representation.First();b!=null; b=b.Next(),i++)
            {
                var c = b.key();
                var d = b.value();
                var tv = r[c];
                var n = cx.NameFor(c);
                if (tv == null)
                    continue;
                var p = tv.dataType;
                var m = (tr.objects[c] as DBObject).Meta();
                if (tv != null && !tv.IsNull && m != null && m.Has(Sqlx.ATTRIBUTE))
                    sb.Append(" " + n + "=\"" + tv.ToString() + "\"");
                else if (tv != null && !tv.IsNull)
                {
                    ss[i] = "<" + n + " type=\"" + p.ToString() + "\">" +
                        p.Xml(tr, cx, defpos, tv) + "</" + n + ">";
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
                if (cd.prim != Sqlx.TYPE && cd.prim != Sqlx.ARRAY && cd.prim != Sqlx.MULTISET)
                    tn = cd.SystemType.Name;
                if (cd.prim == Sqlx.ARRAY || cd.prim == Sqlx.MULTISET)
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
                if (cd.prim != Sqlx.ARRAY && cd.prim != Sqlx.MULTISET)
                    continue;
                cd = cd.elType;
                var di = (ObInfo)db.role.infos[cd.defpos];
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
        internal ObInfo ColFor(Context cx,string nm)
        {
            for (var b=domain.representation.First();b!=null;b=b.Next())
            {
                var p = b.key();
                var ci = (ObInfo)cx.db.role.infos[p];
                if (ci?.name == nm)
                    return ci;
            }
            return null;
        }
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
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
    }
}