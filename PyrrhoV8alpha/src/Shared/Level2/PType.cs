using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using Pyrrho.Level5;
using System.Reflection.Metadata;
using System.Text;
using System.Xml;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2026
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
        internal string metastring = ""; // inline, for the current role
        internal TMetadata metadata = TMetadata.Empty; // ditto
        internal virtual long defpos => ppos;
        /// <summary>
        /// Constructor: A user-defined type definition from the Parser.
        /// </summary>
        /// <param name="t">The PType type</param>
        /// <param name="nm">The name of the new type</param>
        /// <param name="dt">The representation datatype</param>
        /// <param name="db">The local database</param>
        protected PType(Type t, string nm, Domain dm, CTree<Domain, bool> un, long ns, long pp, Context cx)
            : base(t, pp, cx, nm, dm, ns)
        {
            name = nm;
            var dm1 = (t == Type.EditType) ? dm : (Domain)dm.Relocate(pp);
            if (dm1 is UDType ne && dm1.defpos != dm.defpos)
                ne.Fix(cx);
            var rt = CTree<int, long>.Empty;
            var rs = CTree<long, Domain>.Empty;
            var cs = CTree<string, long>.Empty;
            for (var b = un.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key().defpos] is Table sp)
                    for (var c = sp.rowType.First(); c != null; c = c.Next())
                        if (cx.db.objects[c.value()] is TableColumn tc
                            && tc.NameFor(cx) is string cn
                            && !rs.Contains(tc.defpos))
                        {
                            cs += (cn, tc.defpos);
                            rt += ((int)rt.Count, tc.defpos);
                            rs += (tc.defpos, tc.domain);
                        }
            for (var b = dm1.rowType.First(); b != null; b = b.Next())
                if (cx.db.objects[b.value()] is TableColumn tc
                    && tc.NameFor(cx) is string cn
                    && !rs.Contains(tc.defpos))
                {
                    if (cs[cn] is long mp && cx.db.objects[mp] is TableColumn mc)
                        cx.MergeColumn(tc.defpos, mp);
                    else
                    {
                        rt += ((int)rt.Count, tc.defpos);
                        rs += (tc.defpos, tc.domain);
                    }
                }
            under = un;
            dataType = dm1 + (ObInfo.Name, nm) + (Domain.RowType, rt) + (Domain.Representation, rs);
            if (un.Count != 0L)
                dataType += (Domain.Under, cx.FixTDb(un));
            cx.db += dataType;
        }
        /// <summary>
        /// hack for ad-hoc Union or Row type
        /// </summary>
        /// <param name="nm"></param>
        /// <param name="dm"></param>
        /// <param name="pp"></param>
        /// <param name="cx"></param>
        public PType(string nm, Domain dm,long pp, Context cx)
            : base(Type.PType,pp,cx,nm,dm,pp)
        {
            dataType = dm + (DBObject.Infos,dm.infos+(cx.role.defpos,new ObInfo(name))) + (ObInfo.Name,name);
            ifNeeded = true;
        }
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
            under = wr.cx.FixTDb(x.under);
        }
        public override long Dependent(Writer wr, Transaction tr)
        {
            for (var b = under.First(); b != null; b = b.Next())
                if (!Committed(wr, b.key().defpos)) return b.key().defpos;
            return -1L;
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
            wr.PutLong(under.Last()?.key()?.defpos?? -1L); // If Count>1 we use PType2 (allowed for Node types only)
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
            var un = rdr.GetLong(); // the first supertype or -1 if there are none
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
            var st = rdr.GetLong(); // a relic of the past
            var dt = dataType;
            m = m + (Domain.Representation, dt.representation) + (Domain.RowType, dt.rowType);
            var nn = Names.Empty;
            var ns = Names.Empty;
            for (var b = dt.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && rdr.context.NameFor(p) is string n)
                {
                    ns += (n, (0L,p));
                    nn += (n, (0L,p));
                }
            m = m + (ObInfo.Name, name) + (Domain.Kind, k);
            if (dt.super.Count > 0)
                m += (Domain.Under, dt.super);
            if (un > 0)
            { // it can happen that under is more recent than dt (EditType), so be careful
                var un1 = (Table)(rdr.context.db.objects[un] ?? Domain.TypeSpec);
                var ui = un1.infos[rdr.context.role.defpos];
                var rs = dt.representation;
                for (var b = ui?.names.First(); b != null; b = b.Next())
                    if (ns[b.key()].Item2 is long p && p > dt.defpos)
                    {
                        rs -= p;
                        ns -= b.key();
                    }
                var tr = CList<long>.Empty;
                for (var b = ns.First(); b != null; b = b.Next())
                    tr += b.value().Item2;
                var nrt = CTree<int,long>.Empty;
                if (dt != null)
                    m += (Domain.Representation, rs);
                for (var b = tr.First(); b is not null; b = b.Next())
                    if (b.value() is long p && rs.Contains(p))
                        nrt += ((int)nrt.Count, p);
                m += (Domain.RowType, nrt);
                m += (Domain.Under, under);
            }
            oi += (ObInfo._Names, ns);
            m += (DBObject.Infos, new BTree<long, ObInfo>(rdr.context.role.defpos, oi));
            switch(k)
            {
                case Qlx.TYPE: dataType = new UDType(defpos, m); break;
                case Qlx.NODETYPE:
                    metadata = new TMetadata(new CTree<Qlx, TypedValue>(Qlx.NODETYPE, new TChar("NODETYPE")));
                    metastring = "NODETYPE";
                    dataType = (UDType)rdr.context.Add(new UDType(defpos, m));
                    dataType += (DBObject.Infos,dataType.infos+(rdr.context.role.defpos, oi));
                    break;
                case Qlx.EDGETYPE:
                    metadata = new TMetadata(new CTree<Qlx, TypedValue>(Qlx.EDGETYPE, new TChar("EDGETYPE")));
                    metastring = "EDGETYPE";
                    dataType = (UDType)rdr.context.Add(new UDType(defpos, m));
                    dataType += (DBObject.Infos,dataType.infos+(rdr.context.role.defpos, oi));
                    break;
                default:
                    dataType = Domain.Null; break;
            }
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
                var cm = " Under: [";
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
            var ps = CTree<long, Domain>.Empty;
            var pn = BTree<string, (long,long)>.Empty;
            var pt = CTree<int,long>.Empty;
            var cr = CTree<long, CTree<long, bool>>.Empty;
            var pr = metastring;
            var pm = metadata;
            for (var b = under.First(); b != null; b = b.Next())
                if (cx.db.objects[b.key().defpos] is UDType so)
                {
                    if (so.defpos >= Transaction.Analysing
                    && so.defpos < Transaction.Executables)
                        so = cx.db.objects[cx.role.dbobjects[so.name ?? ""]] as UDType
                        ?? throw new PEException("PE070405");
                    un += (so, true);
                    for (var c = so.rowType.First();c!=null;c=c.Next())
                    {
                        var p = c.value();
                        if (ps[p] is not Domain pd)
                            pt += ((int)pt.Count, p);
                        else if (so.representation[p]?.kind != pd.kind)
                            throw new DBException("PE20921");
                    }
                    ps += so.representation;
                    for (var c = so.colRefs.First(); c != null; c = c.Next())
                        cr += (c.key(), cr[c.key()] ?? CTree<long, bool>.Empty + c.value());
                    if (so.infos[cx.role.defpos] is ObInfo si)
                    {
                        pn += si.names ?? Names.Empty;
                        pm += si.metadata;
                    }
                }
            if (dataType.kind == Qlx.Null && under != CTree<Domain, bool>.Empty)
            {
                dataType = new UDType(defpos, new BTree<long, object>(Domain.Kind, Qlx.UNION));
            }
            dataType += (Domain.Under, un);
            dataType += (Domain.ColRefs, cr);
            under = un;
            var st = CTree<long, bool>.Empty;
            for (var b = dataType.subtypes?.First(); b != null; b = b.Next())
                if (cx.obs[b.key()] is UDType so)
                {
                    if (so.defpos >= Transaction.Analysing
                    && so.defpos < Transaction.Executables)
                        so = cx.db.objects[cx.role.dbobjects[so.name ?? ""]] as UDType
                        ?? throw new PEException("PE070405");
                    st += (so.defpos, true);
                }
            if (st.CompareTo(dataType.subtypes) != 0)
                dataType += (Domain.Subtypes, st - defpos);
            for (var b = dataType.representation.First(); b != null; b = b.Next())
                if (cx.NameFor(b.key()) is string n && !pn.Contains(n))
                {
                    ps += (b.key(), b.value());
                    pn += (n, (0L,b.key()));
                    pt += ((int)pt.Count, b.key());
                }
            if (pt.CompareTo(dataType.rowType)!=0 || ps.CompareTo(dataType.representation)!=0)
                dataType = dataType+(Domain.RowType,pt)+(Domain.Representation,ps)
                    + (Domain.Display,(int)pt.Count);
            ro += (Role.DBObjects, ro.dbobjects + (name, defpos));
            var ss = CTree<Domain, bool>.Empty;
            var oi = dataType.infos[cx.role.defpos];
            var ns = Names.Empty;
            if (oi is null || oi.name != name || oi.names == Names.Empty)
            {
                var priv = Grant.Privilege.Owner | Grant.Privilege.Insert | Grant.Privilege.Select |
                    Grant.Privilege.Update | Grant.Privilege.Delete |
                    Grant.Privilege.GrantDelete | Grant.Privilege.GrantSelect |
                    Grant.Privilege.GrantInsert |
                    Grant.Privilege.Usage | Grant.Privilege.GrantUsage;
                oi = new ObInfo(name, priv);
                oi += (ObInfo.SchemaKey, ppos);
                ns = dataType.AllCols(cx);
                var no = Names.Empty;
                for (var b = dataType.First(); b != null; b = b.Next())
                    if (cx.NameFor(b.value()) is string n)
                        no += (n, ns[n]);
                oi += (ObInfo._Names, no);
            }
            var ons = oi.names;
            var oms = oi.methodInfos;
            dataType += (DBObject.Infos, dataType.infos + (cx.role.defpos, oi));
            dataType += (ObInfo._Names, ons);
            (cx,var o) = dataType.Add(cx, pm);
            dataType = (Domain)o;
            if (dataType is UDType ut)
                for (var b = ut.super.First(); b != null; b = b.Next())
                    if ((cx.db.objects[b.key().defpos] ?? cx.obs[b.key().defpos]) is UDType tu)
                    {
                        ss -= tu;
                        dataType += (Table.TableChecks, ut.tableChecks + tu.tableChecks);
                        var ug = ut.triggers;
                        for (var c = tu.triggers.First(); c != null; c = c.Next())
                            ug += (c.key(), (ug[c.key()] ?? CTree<long, bool>.Empty) + c.value());
                        if (ug!=ut.triggers)
                            dataType += (Table.Triggers, ug);
                        tu += (Domain.Subtypes, tu.subtypes + (defpos, true));
                        tu += (Table.TableRows, tu.tableRows + ut.tableRows);
                        var ui = tu.infos[cx.role.defpos];
                        var tn = ui?.names ?? Names.Empty;
                        var tm = ui?.methodInfos ?? CTree<string,CTree<CList<Domain>, long>>.Empty; 
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
                        oms += tm;
                    }
            oi += (ObInfo._Names, ons);
            oi += (ObInfo.MethodInfos, oms);
            oi += (ObInfo._Metadata, pm);
            var os = new BTree<long, ObInfo>(Database._system.role.defpos, oi)
                + (ro.defpos, oi);
            if (ss?.Count > 0L)
                dataType += (Domain.Under, ss);
            dataType = dataType + (DBObject.Infos, os) + (DBObject.Definer, cx.role.defpos)
                + (ObInfo._Names,ons);
            cx.Add(dataType);
            cx.db += dataType;
            if (oi.metadata.Contains(Qlx.NODETYPE))
               ro += (Role.NodeTypes, ro.nodeTypes + (name, defpos));
           if (oi.metadata.Contains(Qlx.EDGETYPE))
               ro += (Role.EdgeTypes, ro.edgeTypes + (name, defpos));
           if (oi.metadata.Contains(Qlx.GRAPH))
               ro += (Role.Graphs, ro.graphs + (name, defpos)); 
            if (cx.db.format < 51)
                ro += (Role.DBObjects, ro.dbobjects + ("" + defpos, defpos));
            cx.db = cx.db + ro + dataType;
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            dataType = (Domain)dataType.Fix(cx);
            (dataType as UDType)?.AddNodeOrEdgeType(cx);
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
        public PType2(Type t, Reader rdr) : base(t, rdr) { }
        public PType2(string nm, Domain dm, CTree<Domain, bool> un, long ns, long pp, Context cx) 
            : base(Type.PType2, nm, dm, un, ns, pp, cx) { }
        public PType2(Type t,string nm, Domain dm, CTree<Domain, bool> un, long ns, long pp, Context cx)
            : base(t, nm, dm, un, ns, pp, cx) { }
        protected PType2(PType2 x, Writer wr) : base(x, wr) 
        {
            under = wr.cx.FixTDb(x.under);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PType2(this, wr);
        }
        public override void Deserialise(Reader rdr)
        {
            var n = rdr.GetInt(); 
            for (int i = 0; i < n; i++) 
            {
                var p = rdr.GetLong();
                under += ((Domain)(rdr.context.db.objects[p] ?? throw new DBException("2E203")), true);
            }
            base.Deserialise(rdr);
        }
        public override void Serialise(Writer wr)
        {
            var n = (int)under.Count;
            wr.PutInt(n);
            for (var b = under.First(); b != null && n-- > 0; b = b.Next())
                wr.PutLong(wr.cx.Fix(b.key().defpos));
            base.Serialise(wr);
        }
    }
}
