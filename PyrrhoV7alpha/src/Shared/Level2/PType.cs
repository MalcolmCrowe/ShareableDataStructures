using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;
using System.Security.Authentication.ExtendedProtection;
using System.Xml;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
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
	internal class PType : PDomain
	{
        internal UDType under;
        /// <summary>
        /// Constructor: A user-defined type definition from the Parser
        /// </summary>
        /// <param name="t">The PType type</param>
        /// <param name="nm">The name of the new type</param>
        /// <param name="dt">The representation datatype</param>
        /// <param name="db">The local database</param>
        protected PType(Type t, Ident nm, Domain dm, long pp, Context cx)
            : base(t, nm.ident, new UDType(pp,dm), pp, cx)
        { }
        public PType(Ident nm, Domain dm, long pp, Context cx)
            : this(Type.PType, nm, dm, pp, cx) { }
        /// <summary>
        /// Constructor: A user-defined type definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PType(ReaderBase rdr) : base(Type.PType,rdr) 
        { }
        /// <summary>
        /// Constructor: A user-defined type definition from the buffer
        /// </summary>
        /// <param name="t">The PType type</param>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		protected PType(Type t, ReaderBase rdr) : base(t,rdr) {}
        protected PType(PType x, Writer wr) : base(x, wr)
        {
            if (x.under!=null)
                under = (UDType)wr.Fixed(under.defpos);
            domain = new UDType(this);
            wr.cx._Add(domain);
            wr.cx.db += (Database.Types, wr.cx.db.types + (domain, defpos));
        }
        protected override PDomain New(Writer wr)
        {
            return new PType(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
		public override void Serialise(Writer wr)
		{
            wr.PutLong((under!=null)?wr.cx.db.types[under]:-1L);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(ReaderBase rd)
        {
            underdefpos = rd.GetLong();
            base.Deserialise(rd);
            domain = new UDType(this);
            if (rd is Reader rdr)
            {
                if (underdefpos != -1)
                    under = (UDType)rdr.context.db.objects[underdefpos];
                rdr.context.db += (ppos, domain);
            }
        }
        /// <summary>
        /// A readable version of the Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString() 
		{
            string a = "PType ";
            a += domain.ToString();
			return a;
		}
        public override DBException Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch(that.type)
            {
                case Type.Drop:
                        if (db.types[under] == ((Drop)that).delpos)
                        return new DBException("40011", ppos, that, ct);
                    break;// base class has other reasons for concern
                case Type.Change:
                    if (db.types[under] == ((Change)that).affects)
                        return new DBException("40021", ppos, that, ct);
                    break;// base class has other reasons for concern
            }
            return base.Conflicts(db, cx, that, ct);
        }
        internal override void Install(Context cx, long p)
        {
            var ro = cx.db.role;
            var udt = (UDType)domain;
            var priv = Grant.Privilege.Usage | Grant.Privilege.GrantUsage;
            var oi = new ObInfo(ppos, domain.name, domain, priv);
            ro = ro + (Role.DBObjects, ro.dbobjects + (domain.name, ppos));
            ro += (oi, true);
            if (cx.db.format < 51)
                ro += (Role.DBObjects, ro.dbobjects + ("" + ppos, ppos));
            cx.db = cx.db + (ro, p) + (ppos, udt, p);
            cx.db += (Database.Types, cx.db.types + (udt-Domain.Representation, ppos));
            var tt = cx.db.typeTracker[defpos] ?? BTree<long, Domain>.Empty
    + (ppos, udt);
            cx.db += (Database.TypeTracker, cx.db.typeTracker + (defpos, tt));
            cx.db += (Database.Log, cx.db.log + (ppos, type));
        }
    }
    internal class PType1 : PType // no longer used
    {
        /// <summary>
        /// Constructor: A user-defined type definition from the Parser
        /// </summary>
        /// <param name="nm">The name of the new type</param>
        /// <param name="wu">The =WITH uri in the representation if any</param>
        /// <param name="un">The supertype if specified</param>
        /// <param name="dt">The representation datatype</param>
        /// <param name="db">The local database</param>
        public PType1(Ident nm, Domain ud, long pp, Context cx)
            : base(Type.PType1, nm, ud, pp, cx)
        {  }
        /// <summary>
        /// Constructor: A user-defined type definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PType1(ReaderBase rdr) : base(Type.PType1,rdr) {}
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
        public override void Deserialise(ReaderBase rdr)
        {
            string withuri = rdr.GetString();
            base.Deserialise(rdr);
        }
    }
 }
