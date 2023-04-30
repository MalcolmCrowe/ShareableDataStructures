using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using System.Globalization;
using System.Runtime.Intrinsics.Arm;
using System.Text;
using System.Xml;
using System.Xml.Linq;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2023
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

namespace Pyrrho.Level2
{
	/// <summary>
	/// Basic structured type support
	/// Similar information is specified for a Type as for a Domain with the following additions
	///		under	subtype info: may be -1 if not a subtype
	///		representation	uses structDef field in Domain
	///	so attributes are TableColumns of the referenced PTable
	/// </summary>
	internal class PType : Defined
	{
        internal Domain? under = null;
        internal virtual long defpos => ppos;
        /// <summary>
        /// Constructor: A user-defined type definition from the Parser.
        /// </summary>
        /// <param name="t">The PType type</param>
        /// <param name="nm">The name of the new type</param>
        /// <param name="dt">The representation datatype</param>
        /// <param name="db">The local database</param>
        protected PType(Type t, Ident nm, UDType dm, Domain? un, long pp, Context cx)
            : base(t, pp, cx, nm.ident, Grant.AllPrivileges)
        {
            name = nm.ident;
            var dm1 = (t==Type.EditType)? dm: (Domain)dm.Relocate(pp);
            dataType = dm1 + (ObInfo.Name,nm.ident);
            var ps = ((Transaction)cx.db).physicals;
            var pt = (PTable)(ps[dataType.structure] ?? new PTable(nm.ident,dataType,pp,cx));
            if (t==Type.PNodeType||t==Type.PEdgeType)
                pt.nodeType = dm.defpos;
            cx.db += (Transaction.Physicals, ps + (pt.ppos, pt));
            under = un;
        }
        public PType(Ident nm, UDType dm, Domain? un, long pp, Context cx)
            : this(Type.PType, nm, dm, un, pp, cx) { }
        /// <summary>
        /// Constructor: A user-defined type definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PType(Reader rdr) : base(Type.PType,rdr) 
        {
            dataType = Domain.TypeSpec;
        }
        /// <summary>
        /// Constructor: A user-defined type definition from the buffer
        /// </summary>
        /// <param name="t">The PType type</param>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		protected PType(Type t, Reader rdr) : base(t,rdr) 
        { }
        protected PType(PType x, Writer wr) : base(x, wr)
        {
            name = wr.cx.NewNode(wr.Length,x.name.Trim(':'));
            if (x.name.EndsWith(':'))
                name += ':';
            under = (UDType?)x.under?.Fix(wr.cx);
            dataType = (UDType)dataType.Fix(wr.cx);
        }
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (under != null && !Committed(wr, under.defpos)) return under.defpos;
            for (var b = dataType.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && p <= Transaction.Executables && !Committed(wr, p))
                        return p;
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
            wr.PutLong(under?.defpos??-1L); 
            // copied from PDomain.Serialise
            wr.PutString(name);
            wr.PutInt((int)dataType.kind);
            wr.PutInt(dataType.prec);
            wr.PutInt(dataType.scale);
            wr.PutInt((int)dataType.charSet);
            wr.PutString(dataType.culture.Name);
            wr.PutString(dataType.defaultString);
            wr.PutLong(wr.cx.Fix(dataType.structure));
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            var un = rdr.GetLong();
            name = rdr.GetString();
            var m = BTree<long, object>.Empty;
            var k = (Sqlx)rdr.GetInt();
            m = m + (Domain.Precision, rdr.GetInt())
                + (Domain.Scale, rdr.GetInt())
                + (DBObject.Definer, rdr.context.role.defpos)
                + (Domain.Charset, (CharSet)rdr.GetInt())
                + (Domain.Culture, PDomain.GetCulture(rdr.GetString()));
            var oi = new ObInfo(name, Grant.AllPrivileges);
            var ds = rdr.GetString();
            if (ds.Length > 0
                && k == Sqlx.CHAR && ds[0] != '\'')
            {
                ds = "'" + ds + "'";
                m += (Domain.DefaultString, ds);
            }
            var st = rdr.GetLong();
            var ts = rdr.context._Ob(st) as Table;
            if (ts != null)
            {
                rdr.context.Add(ts.framing);
                ts += (Table._NodeType, ppos);
                rdr.context.Add(ts);
                rdr.context.db += (ts, rdr.context.db.loadpos);
            }
            var dt = ts?.domain??Domain.Content;
            m = m + (Domain.Representation, dt.representation) + (Domain.RowType, dt.rowType);
            var ns = BTree<string,(int,long?)>.Empty;
            for (var b = dt.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && rdr.context.NameFor(p) is string n)
                    ns += (n, (b.key(), p));
            m = m + (ObInfo.Name, name) + (Domain.Kind, k) + (Domain.Structure, st);
            if (un > 0)
            { // it can happen that under is more recent than dt (EditType), so be careful
                under = (UDType)(rdr.context.db.objects[un] ?? Domain.TypeSpec);
                for (var b = under.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p && rdr.context.NameFor(p) is string n)
                    {
                        if (ns.Contains(n))
                        {
                            var (i, iq) = ns[n];
                            if (iq == null || p > iq)
                                ns += (n, (i, p));
                        }
                        else
                            ns += (n, ((int)ns.Count, p));
                    }
                var tr = BTree<int, long?>.Empty;
                for (var b = ns.First(); b != null; b = b.Next())
                    tr += b.value();
                var nrt = BList<long?>.Empty;
                for (var b = tr.First(); b is not null; b = b.Next())
                    nrt += b.value();
                if (dt != null)
                    m += (Domain.Representation, dt.representation + under.representation);
                m += (Domain.RowType, nrt);
                m += (UDType.Under, under);
            }
            oi += (ObInfo.Names, ns);
            m += (DBObject.Infos, new BTree<long, ObInfo>(rdr.context.role.defpos, oi));
            dataType = k switch
            {
                Sqlx.TYPE => new UDType(defpos, m),
                Sqlx.NODETYPE => new NodeType(defpos, m),
                Sqlx.EDGETYPE => new EdgeType(defpos,m),
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
            if (under != null)
            { sb.Append(" Under: "); sb.Append(DBObject.Uid(under.defpos)); }
            return sb.ToString();
		}
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            var nm = dataType.name;
            switch(that.type)
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
                    var tn = ((PDomain)that).domain.name;
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
        internal override DBObject Install(Context cx, long p)
        {
            var ro = cx.role;
            var priv = Grant.Privilege.Usage | Grant.Privilege.GrantUsage;
            var oi = new ObInfo(name, priv);
            oi += (ObInfo.SchemaKey, p);
            var ns = BTree<string, (int,long?)>.Empty;
            for (var b = dataType.rowType.First(); b != null; b = b.Next())
                if (b.value() is long bp && cx.obs[bp] is TableColumn tc)
                    ns += (tc.infos[tc.definer]?.name ?? "??", (b.key(),tc.defpos));
            oi += (ObInfo.Names, ns);
            ro += (Role.DBObjects, ro.dbobjects + (name, defpos));
            var os = new BTree<long, ObInfo>(Database._system.role.defpos, oi)
                + (ro.defpos, oi);
            var ts = cx.db.types - dataType;
            if ((dataType as UDType)?.super is UDType s && cx.db.objects[s.defpos] is UDType su
                && cx.db.objects[s.structure] is Table tu)
            {
                su += (UDType.Subtypes, su.subtypes + (ppos,true));
                cx.db += (su, p);
                ts += (su, su.defpos);
                tu += (DBObject._Domain, su);
                cx.db += (tu.defpos, tu);
                for (var b = su.subtypes.First(); b != null; b = b.Next())
                    if (cx.db.objects[b.key()] is UDType ad && cx.db.objects[ad.structure] is Table at)
                    {
                        ad += (UDType.Under, su);
                        at += (DBObject._Domain, ad); // but not LastChange?
                        cx.db += (at.defpos, at);
                        cx.db += (ad.defpos, ad);
                    }
                dataType += (UDType.Under, su);
            }
            dataType = dataType + (DBObject.Infos, os) + (DBObject.Definer, cx.role.defpos);
            cx.Add(dataType);
            if (cx.db.format < 51)
                ro += (Role.DBObjects, ro.dbobjects + ("" + defpos, defpos));
            ts += (dataType, defpos);
            if (cx.db.objects[dataType.structure] is Table tb)
            {
                var ti = tb.infos[cx.role.defpos] ?? new ObInfo(name + ":");
                if (this is PNodeType)
                {
                    tb += (Table._NodeType, defpos);
                    ti += (ObInfo._Metadata, ti.metadata + (dataType.kind, TNull.Value));
                }
                tb += (DBObject._Domain, dataType);
                ti += (ObInfo.Names, ns);
                tb += (DBObject.Infos, tb.infos + (cx.role.defpos, ti));
                cx.db += (tb, p);
            }
            cx.db = cx.db + (ro, p) + (ppos, dataType, p) + (Database.Types,ts);
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            
            return dataType;
        }
    }
    internal class PType1 : PType // no longer used
    {
        /// <summary>
        /// Constructor: A user-defined type definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PType1(Reader rdr) : base(Type.PType1,rdr) {}
        protected PType1(PType1 x, Writer wr) : base(x, wr)
        { }
        protected override Physical Relocate(Writer wr)
        {
            return new PType1(this, wr);
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
 }
