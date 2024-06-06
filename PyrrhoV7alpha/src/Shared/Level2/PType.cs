using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Level5;
using System.Text;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level2
{
    /// <summary>
    /// Basic structured type support
    /// Similar information is specified for a Type as for a Domain with the following additions
    ///		under	subtype info: may be -1 if not a subtype
    ///		representation	uses structDef field in Domain
    ///	so attributes are TableColumns of the referenced PTable
    /// </summary>
    internal class PType : Compiled
    {
        internal CTree<Domain, bool> under = CTree<Domain, bool>.Empty;
        internal virtual long defpos => ppos;
        /// <summary>
        /// Constructor: A user-defined type definition from the Parser.
        /// </summary>
        /// <param name="t">The PType type</param>
        /// <param name="nm">The name of the new type</param>
        /// <param name="dt">The representation datatype</param>
        /// <param name="db">The local database</param>
        protected PType(Type t, string nm, UDType dm, CTree<Domain, bool> un, long ns, long pp, Context cx)
            : base(t, pp, cx, nm, dm, ns)
        {
            name = nm;
            var dm1 = (t == Type.EditType) ? dm : (Domain)dm.Relocate(pp);
            if (dm1 is EdgeType ne && dm1.defpos != dm.defpos && ne.leavingType > 0 && ne.arrivingType > 0)
                ne.Fix(cx);
            var rt = BList<long?>.Empty;
            var rs = CTree<long,Domain>.Empty;
            for (var b = un.First(); b != null; b = b.Next())
                if (b.key() is Table sp)
                    for (var c = sp.rowType.First(); c != null; c = c.Next())
                        if (cx.db.objects[c.value() ?? -1L] is TableColumn tc
                            && !rs.Contains(tc.defpos))
                        {
                            rt += tc.defpos;
                            rs += (tc.defpos, tc.domain);
                        } 
            for (var b=dm1.rowType.First(); b != null;b=b.Next())
                if (cx.db.objects[b.value() ?? -1L] is TableColumn tc
                    && !rs.Contains(tc.defpos))
                {
                    rt += tc.defpos;
                    rs += (tc.defpos, tc.domain);
                }
            dataType = dm1 + (ObInfo.Name, nm) + (Domain.RowType,rt) + (Domain.Representation,rs);
            under = un;
            if (un.Count != 0L)
                dataType += (Domain.Under, un);
        }
        public PType(string nm, UDType dm, CTree<Domain, bool> un, long ns, long pp, Context cx)
            : this(Type.PType, nm, dm, un, ns, pp, cx) { }
        /// <summary>
        /// Constructor: A user-defined type definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PType(Reader rdr) : base(Type.PType, rdr)
        {
            dataType = Domain.TypeSpec;
        }
        /// <summary>
        /// Constructor: A user-defined type definition from the buffer
        /// </summary>
        /// <param name="t">The PType type</param>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		protected PType(Type t, Reader rdr) : base(t, rdr)
        { }
        protected PType(PType x, Writer wr) : base(x, wr)
        {
            name = wr.cx.NewNode(wr.Length, x.name.Trim(':'));
            if (x.name.EndsWith(':'))
                name += ':';
            under = wr.cx.FixTDb(x.under);
        }
        public override long Dependent(Writer wr, Transaction tr)
        {
            for (var b = under.First(); b != null; b = b.Next())
                if (!Committed(wr, b.key().defpos)) return b.key().defpos;
            return -1L;
        }
        public override (Transaction?, Physical) Commit(Writer wr, Transaction? tr)
        {
            var un = CTree<Domain, bool>.Empty;
            for (var b=under.First();b!=null;b=b.Next())
            {
                var k = b.key();
                if (tr?.objects[b.key().defpos] is Domain d && d.dbg > k.dbg)
                    k = d;
                un += (k,true);
            }
            under = un;
            return base.Commit(wr, tr);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PType(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
		public override void Serialise(Writer wr)
        {
            wr.PutLong(under.Last()?.key().defpos ?? -1L); // If Count>1 we use PType2 (allowed for Node types only)
            // copied from PDomain.Serialise
            wr.PutString(name);
            wr.PutInt((int)dataType.kind);
            wr.PutInt(dataType.prec);
            wr.PutInt(dataType.scale);
            wr.PutInt((int)dataType.charSet);
            wr.PutString(dataType.culture.Name);
            wr.PutString(dataType.defaultString);
            wr.PutLong(-1L);
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            var un = rdr.GetLong(); // the first supertype or -1 is there are none
            if (un > 0 && rdr.context.db.objects[un] is Domain d)
                under += (d, true);
            name = rdr.GetString();
            var m = dataType.mem; // the Old domain for EditType, otherwise Content + PNodeType and PEdgeType things
            var k = (Qlx)rdr.GetInt();
            m = m + (Domain.Precision, rdr.GetInt())
                + (Domain.Scale, rdr.GetInt())
                + (DBObject.Definer, rdr.context.role.defpos)
                + (Domain.Charset, (CharSet)rdr.GetInt())
                + (Domain.Culture, PDomain.GetCulture(rdr.GetString()));
            var oi = new ObInfo(name, Grant.AllPrivileges);
            var ds = rdr.GetString();
            if (ds.Length > 0
                && k == Qlx.CHAR && ds[0] != '\'')
            {
                ds = "'" + ds + "'";
                m += (Domain.DefaultString, ds);
            }
            var st = rdr.GetLong(); // a relic of the past
            var dt = dataType;
            m = m + (Domain.Representation, dt.representation) + (Domain.RowType, dt.rowType);
            var nn = CTree<string, long>.Empty;
            var ns = BTree<string, (int, long?)>.Empty;
            for (var b = dt.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && rdr.context.NameFor(p) is string n)
                {
                    ns += (n, (b.key(), p));
                    nn += (n, p);
                }
            m = m + (ObInfo.Name, name) + (Domain.Kind, k);
            if (dt.super.Count > 0)
                m += (Domain.Under, dt.super);
            if (un > 0)
            { // it can happen that under is more recent than dt (EditType), so be careful
                var un1 = (UDType)(rdr.context.db.objects[un] ?? Domain.TypeSpec);
                var ui = un1.infos[rdr.context.role.defpos];
                var rs = dt.representation;
                for (var b = ui?.names.First(); b != null; b = b.Next())
                    if (ns[b.key()].Item2 is long p && p > dt.defpos)
                    {
                        rs -= p;
                        ns -= b.key();
                    }
                var tr = BTree<int, long?>.Empty;
                for (var b = ns.First(); b != null; b = b.Next())
                    tr += b.value();
                var nrt = BList<long?>.Empty;
                if (dt != null)
                    m += (Domain.Representation, rs);
                for (var b = tr.First(); b is not null; b = b.Next())
                    if (b.value() is long p && rs.Contains(p))
                        nrt += p;
                m += (Domain.RowType, nrt);
                m += (Domain.Under, under);
            }
            oi += (ObInfo.Names, ns);
            m += (DBObject.Infos, new BTree<long, ObInfo>(rdr.context.role.defpos, oi));
            dataType = k switch
            {
                Qlx.TYPE => new UDType(defpos, m),
                Qlx.NODETYPE => new NodeType(defpos, m),
                Qlx.EDGETYPE => new EdgeType(defpos, m),
                _ => Domain.Null
            };
            base.Deserialise(rdr);
        }
        /// <summary>
        /// A readable version of the Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            if (under.Count > 0)
            {
                var cm = "Under: [";
                for (var b = under.First(); b != null; b = b.Next())
                {
                    sb.Append(cm); cm = ","; sb.Append(DBObject.Uid(b.key().defpos));
                }
                sb.Append(']');
            }
            return sb.ToString();
        }
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            var nm = dataType.name;
            switch (that.type)
            {
                case Type.PType:
                case Type.PType1:
                case Type.PNodeType:
                case Type.PEdgeType:
                    if (nm == ((PType)that).dataType.name)
                        return new DBException("40022", nm, that, ct);
                    break;
                case Type.PDomain1:
                case Type.PDomain:
                    var tn = ((PDomain)that).name;
                    if (nm == tn)
                        return new DBException("40022", nm, tn, ct);
                    break;
                case Type.PTable:
                case Type.PTable1:
                    if (dataType.name == ((PTable)that).name)
                        return new DBException("40032", nm, that, ct);
                    break;
                case Type.PView:
                    if (nm == ((PView)that).name)
                        return new DBException("40032", nm, that, ct);
                    break;
                case Type.PRole1:
                case Type.PRole:
                    if (nm == ((PRole)that).name)
                        return new DBException("40035", nm, that, ct);
                    break;
                case Type.RestView1:
                case Type.RestView:
                    if (nm == ((PRestView)that).name)
                        return new DBException("40032", nm, that, ct);
                    break;
                case Type.Change:
                    if (nm == ((Change)that).name)
                        return new DBException("40032", nm, that, ct);
                    break;
                case Type.Drop:
                    if (ppos == ((Drop)that).delpos)
                        return new DBException("40016", nm, that, ct);
                    break;
            }
            return base.Conflicts(db, cx, that, ct);
        }
        internal override DBObject Install(Context cx)
        {
            var ro = cx.role;
            var un = CTree<Domain, bool>.Empty;
            for (var b = dataType.super.First(); b != null; b = b.Next())
                if (b.key() is UDType so)
                {
                    if (so.defpos >= Transaction.Analysing
                    && so.defpos < Transaction.Executables)
                        so = cx.db.objects[cx.role.dbobjects[so.name ?? ""] ?? -1L] as NodeType
                        ?? throw new PEException("PE070405");
                    un += (so, true);
                }
            if (un.CompareTo(dataType.super)!=0)
            {
                dataType += (Domain.Under, un);
                under = un;
            }
            var st = CTree<long,bool>.Empty;
            for (var b = dataType.subtypes?.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is UDType so){
                    if (so.defpos >= Transaction.Analysing
                    && so.defpos < Transaction.Executables)
                        so = cx.db.objects[cx.role.dbobjects[so.name ?? ""] ?? -1L] as NodeType
                        ?? throw new PEException("PE070405");
                    st += (so.defpos, true);
                }
            if (st.CompareTo(dataType.subtypes)!=0)
                dataType += (Domain.Subtypes, st);
            var ps = CTree<long, bool>.Empty;
            var pn = CTree<string, bool>.Empty;
            for (var b = dataType.representation.First(); b != null; b = b.Next())
                if (cx.NameFor(b.key()) is string n)
                {
                    ps += (b.key(), true);
                    pn += (n, true);
                }
            ro += (Role.DBObjects, ro.dbobjects + (name, defpos));
            var ss = CTree<Domain, bool>.Empty;
            var oi = dataType.infos[cx.role.defpos];
            if (oi is null || oi.name != name || oi.names == BTree<string, (int, long?)>.Empty)
            {
                var priv = Grant.Privilege.Owner | Grant.Privilege.Insert | Grant.Privilege.Select |
                    Grant.Privilege.Update | Grant.Privilege.Delete |
                    Grant.Privilege.GrantDelete | Grant.Privilege.GrantSelect |
                    Grant.Privilege.GrantInsert |
                    Grant.Privilege.Usage | Grant.Privilege.GrantUsage;
                oi = new ObInfo(name, priv);
                oi += (ObInfo.SchemaKey, ppos);
                var ns = dataType.HierarchyCols(cx);
                oi += (ObInfo.Names, ns);
            } 
            var ons = oi.names;
            if (dataType is UDType ut)
                for (var b = ut.super.First(); b != null; b = b.Next())
                    if ((cx.db.objects[b.key().defpos] ?? cx.obs[b.key().defpos]) is UDType tu)
                    {
                        ss -= tu;
                        dataType = tu.Inherit(ut);
                        dataType += (Table.TableChecks, ut.tableChecks + tu.tableChecks);
                        dataType += (Table.Triggers, ut.triggers + tu.triggers);
                        tu += (Domain.Subtypes, tu.subtypes + (defpos, true));
                        tu += (Table.TableRows, tu.tableRows + ut.tableRows);
                        var tn = tu.infos[cx.role.defpos]?.names ?? BTree<string, (int, long?)>.Empty;
                        cx.db += tu;
                        for (var c = tu.subtypes.First(); c != null; c = c.Next())
                            if (cx.db.objects[c.key()] is UDType at)
                            {
                                var sa = at.super;
                                for (var d = at.super.First(); d != null; d = d.Next())
                                    if (d.key().defpos == tu.defpos)
                                        sa -= d.key();
                                at += (Domain.Under, sa + (tu, true));
                                cx.db += (at.defpos, at);
                            }
                        ss += (tu, true);
                        ons += tn;
                    }
            oi += (ObInfo.Names, ons);
            var os = new BTree<long, ObInfo>(Database._system.role.defpos, oi)
                + (ro.defpos, oi);
            dataType += (Domain.Under, ss);
            dataType = dataType + (DBObject.Infos, os) + (DBObject.Definer, cx.role.defpos);
            cx.Add(dataType);
            if (cx.db.format < 51)
                ro += (Role.DBObjects, ro.dbobjects + ("" + defpos, defpos));
            cx.db = cx.db + ro + dataType;
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            if (dataType is NodeType nt) // or EdgeType
            {
                if (nt is EdgeType et && this is PEdgeType pe
                    && cx.db.objects.Contains(pe.leavingType)
                    && cx.db.objects.Contains(pe.arrivingType))
                {
                    et += (EdgeType.LeavingType, pe.leavingType);
                    et += (EdgeType.ArrivingType, pe.arrivingType);
                    nt = et;
                }
                nt.AddNodeOrEdgeType(cx);
                cx.Add(nt);
            }
            dataType =  (Domain)dataType.Fix(cx);
            return dataType;
        }
    }
    internal class PType1 : PType // retained but no longer used
    {
        protected PType1(Type t, string nm, UDType dm, CTree<Domain,bool> un, long ns, long pp, Context cx)
            :base(t,nm,dm,un, ns, pp, cx) { }
        /// <summary>
        /// Constructor: A user-defined type definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PType1(Reader rdr) : base(Type.PType1,rdr) {}
        protected PType1(Type t,Reader rdr) : base(t,rdr) { }
        protected PType1(PType1 x, Writer wr) : base(x, wr)
        { }
        protected override Physical Relocate(Writer wr)
        {
            return new PType1(this, wr);
        }
        public override void Serialise(Writer wr)
        {
            wr.PutString("");
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            rdr.GetString();
            base.Deserialise(rdr);
        }
    }
    internal class PType2 : PType
    {
        public PType2(Reader rdr) : base(Type.PType2, rdr) { }
        public PType2(Type t,Reader rdr): base(t,rdr) { }
        public PType2(string nm, UDType dm, CTree<Domain, bool> un, long ns, long pp, Context cx) 
            : base(Type.PType2, nm, dm, un, ns, pp, cx) { }
        public PType2(Type t,string nm, UDType dm, CTree<Domain, bool> un, long ns, long pp, Context cx)
            : base(t, nm, dm, un, ns, pp, cx) { }
        protected PType2(PType2 x, Writer wr) : base(x, wr) { }
        public override void Deserialise(Reader rdr)
        {
            var n = rdr.GetInt(); // if >=0 there will be another one (added at the end) from the base PType
            for (int i = 0; i < n; i++) 
            {
                var p = rdr.GetLong();
                under += ((Domain)(rdr.context.db.objects[p] ?? throw new DBException("2E203")), true);
            }
            base.Deserialise(rdr);
        }
        public override void Serialise(Writer wr)
        {
            var n = (int)under.Count - 1;
            wr.PutInt(n);
            for (var b = under.First(); b != null && n-- > 0; b = b.Next())
                wr.PutLong(b.key().defpos);
            base.Serialise(wr);
        }
    }
}
