using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using System.Globalization;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
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
        internal Domain under = null;
        internal Domain structure = null;
        internal string name = "";
        /// <summary>
        /// Constructor: A user-defined type definition from the Parser
        /// </summary>
        /// <param name="t">The PType type</param>
        /// <param name="nm">The name of the new type</param>
        /// <param name="dt">The representation datatype</param>
        /// <param name="db">The local database</param>
        protected PType(Type t, Ident nm, Domain dm, Domain un, long pp, Context cx)
            : base(t, pp, cx, dm)
        {
            name = nm.ident;
            under = un;
            structure = dm;
            framing = new Framing(cx);
        }
        public PType(Ident nm, Domain dm, Domain un, long pp, Context cx)
            : this(Type.PType, nm, dm, un, pp, cx) { }
        /// <summary>
        /// Constructor: A user-defined type definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PType(Reader rdr) : base(Type.PType,rdr) 
        { }
        /// <summary>
        /// Constructor: A user-defined type definition from the buffer
        /// </summary>
        /// <param name="t">The PType type</param>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		protected PType(Type t, Reader rdr) : base(t,rdr) {}
        protected PType(PType x, Writer wr) : base(x, wr)
        {
            name = x.name;
            structure = (Domain)x.structure?._Relocate(wr.cx);
            under = (Domain)x.under?._Relocate(wr.cx);
        }
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (under!=null && !Committed(wr, under.defpos))
                return under.defpos;
            for (var b=structure?.rowType.First();b!=null;b=b.Next())
            {
                var p = b.value();
                if (p<=Transaction.Executables && !Committed(wr, p))
                    return p;
            }
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
            if (un > 0)
                under = (Domain)rdr.context.db.objects[un];
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
            var dt = ((ObInfo)rdr.context.db.role.infos[st]).dataType;
            m = m + (Domain.Structure,st)
                + (Domain.Representation,dt.representation)
                + (Domain.RowType, dt.rowType);
            structure = new Domain(ppos, k, m);
            base.Deserialise(rdr);
        }
        /// <summary>
        /// A readable version of the Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString() 
		{
            return "PType " + name + "["+ DBObject.Uid(structure.structure)+"]";
		}
        public override DBException Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            var nm = dataType.name;
            switch(that.type)
            {
                case Type.PType:
                case Type.PType1:
                    if (nm == ((PType)that).dataType.name)
                        return new DBException("40022", nm, that, ct);
                    break;
                case Type.PDomain1:
                case Type.PDomain:
                    if (nm == cx._Dom(((PDomain)that).domain).name)
                        return new DBException("40022", nm, that, ct);
                    break;
                case Type.PTable:
                case Type.PTable1:
                    if (dataType.name == ((PTable)that).name)
                        return new DBException("40032", nm, that, ct);
                    break;
                case Type.PView1:
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
        public override (Transaction, Physical) Commit(Writer wr, Transaction tr)
        {
            var (nt, ph) = base.Commit(wr, tr);
            return (nt,ph);
        }
        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            var udt = new UDType(this, cx);
            var priv = Grant.Privilege.Usage | Grant.Privilege.GrantUsage;
            var oi = new ObInfo(ppos, name, udt, priv);
            oi += (ObInfo.SchemaKey, p);
            var st = CTree<string, long>.Empty;
            for (var b=udt.rowType.First();b!=null;b=b.Next())
            {
                var ci = (ObInfo)ro.infos[b.value()];
                st += (ci.name, b.value());
            }
            ro = ro + (Role.DBObjects, ro.dbobjects + (name, ppos));
            ro += (oi, true);
            var tt = cx.db.role.typeTracker[ppos] ?? CTree<long, (Domain,CTree<string,long>)>.Empty
                + (ppos, (udt,st));
            ro += (Role.TypeTracker, cx.db.role.typeTracker + (ppos, tt));
            if (cx.db.format < 51)
                ro += (Role.DBObjects, ro.dbobjects + ("" + ppos, ppos));
            cx.db = cx.db + (ro, p) + (ppos, udt, p);
            cx.db += (Database.Types, cx.db.types + (udt, ppos));
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
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
            string withuri = rdr.GetString();
            base.Deserialise(rdr);
        }
    }
 }
