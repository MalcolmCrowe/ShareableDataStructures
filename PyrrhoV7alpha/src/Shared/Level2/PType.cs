using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using System.Globalization;
using System.Runtime.Intrinsics.Arm;
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
	internal class PType : Compiled
	{
        internal Domain? under = null;
        internal Domain? structure = null; // Care: this.dataType.structure is a tabledefpos!
        internal virtual long defpos => ppos;
        /// <summary>
        /// Constructor: A user-defined type definition from the Parser.
        /// </summary>
        /// <param name="t">The PType type</param>
        /// <param name="nm">The name of the new type</param>
        /// <param name="dt">The representation datatype</param>
        /// <param name="db">The local database</param>
        protected PType(Type t, Ident nm, UDType dm, Domain? un, long pp, Context cx)
            : base(t, pp, cx, nm.ident, dm, cx.db.nextStmt)
        {
            name = nm.ident;
            dataType += (ObInfo.Name, nm.ident);
            structure = new Domain(dm.defpos,dm.mem + (Domain.Kind,Sqlx.TABLE) - Domain.Structure);
            cx.Add(structure);
            var ps = ((Transaction)cx.db).physicals;
            var pt = (PTable)(ps[dataType.structure] ?? new PTable(nm.ident,structure,cx.db.nextStmt,pp,cx));
            pt.nodeType = dm.defpos;
            pt.framing += (Framing.Obs, pt.framing.obs + (structure.defpos, structure));
            cx.db += (Transaction.Physicals, ps + (pt.ppos, pt));
            under = un;
            framing = Framing.Empty;
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
            name = x.name;
            structure = (Domain?)x.structure?.Fix(wr.cx);
            under = (Domain?)x.under?.Fix(wr.cx);
        }
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (under != null && !Committed(wr, under.defpos)) return under.defpos;
            for (var b = structure?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long p && p <= Transaction.Executables && !Committed(wr, p))
                        return p;
            return -1L;
        }
        public override BList<long?> Dependent(Transaction tr, BList<long?> ds)
        {
            if (under != null && tr.physicals[under.defpos] is PType upt)
                ds += upt.Dependent(tr, ds);
            if (structure != null)
                for (var b = structure.representation.First(); b != null; b = b.Next())
                    if (tr.physicals[b.value().defpos] is PType spt)
                        ds += spt.Dependent(tr, ds);
            return ds;
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
            wr.PutLong(dataType.structure);
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
                + (Domain.Charset, (CharSet)rdr.GetInt())
                + (Domain.Culture, PDomain.GetCulture(rdr.GetString()));
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
            var dt = rdr.context._Dom(ts);
            if (dt != null)
                m = m + (Domain.Representation, dt.representation)
                + (Domain.RowType, dt.rowType);
            structure = new Domain(dt?.defpos ?? -1L, Sqlx.TABLE, m);
            m = m + (ObInfo.Name, name) + (Domain.Kind, k) + (Domain.Structure, st);
            if (un > 0)
            {
                under = (Domain)(rdr.context.db.objects[un] ?? Domain.Null);
                var nrt = under.rowType;
                for (var b = dt?.rowType.First(); b != null; b = b.Next())
                    if (b.value() is long p)
                        nrt += p;
                if (dt != null)
                    m += (Domain.Representation, dt.representation + under.representation);
                m += (Domain.RowType, nrt);
                m += (UDType.Under, under);
            }
            var op = rdr.context.parse;
            rdr.context.parse = ExecuteStatus.Compile;
            dataType = k switch
            {
                Sqlx.TYPE => new UDType(defpos, m),
                Sqlx.NODETYPE => new NodeType(defpos, m),
                Sqlx.EDGETYPE => new EdgeType(defpos,m),
                _ => Domain.Null
            };
            rdr.context.parse = op;
            base.Deserialise(rdr);
        }
        /// <summary>
        /// A readable version of the Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString() 
		{
            var r = GetType().Name + " "+ name;
            if (structure!= null) 
                r +="["+ DBObject.Uid(dataType.structure)+"]";
            if (under!=null)
                r += " Under: " + DBObject.Uid(under.defpos);
            return r;
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
            var ns = BTree<string, long?>.Empty;
            for (var b = structure?.rowType.First(); b != null; b = b.Next())
                if (b.value() is long bp && cx.obs[bp] is TableColumn tc)
                    ns += (tc.infos[tc.definer]?.name ?? "??", tc.defpos);
            oi += (ObInfo.Names, ns);
            ro += (Role.DBObjects, ro.dbobjects + (name, defpos));
            var os = new BTree<long, ObInfo>(Database._system.role.defpos, oi)
                + (ro.defpos, oi);
            var ts = cx.db.types - dataType;
            dataType = (Domain)(dataType.New(defpos, dataType.mem+ (DBObject.Infos, os)+(DBObject.Definer, cx.role.defpos)));
            cx.Add(dataType);
            if (cx.db.format < 51)
                ro += (Role.DBObjects, ro.dbobjects + ("" + defpos, defpos));
            ts += (dataType, defpos);
            if ((dataType as UDType)?.super is UDType s && cx.db.objects[s.defpos] is UDType su)
            {
                su += (UDType.Subtypes, su.subtypes + (ppos,true));
                cx.db += (su, p);
                ts += (su, su.defpos);
            }
            if (cx._Ob(dataType.structure) is Table ut)
            {
                ut += (Table._NodeType, defpos);
                cx.Add(ut);
                cx.db += (ut, cx.db.loadpos);
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
