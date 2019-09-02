using System;
using System.Collections;
using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code 
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

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
        internal long underdefpos;
        /// <summary>
        /// Constructor: A user-defined type definition from the Parser
        /// </summary>
        /// <param name="nm">The name of the new type</param>
        /// <param name="sd">The representation datatype</param>
        /// <param name="pb">The local database</param>
        public PType(string nm, long ud, Domain dt, long u, Transaction tr)
            : this(Type.PType,nm, ud, dt, u, tr)
        {
        }
        /// <summary>
        /// Constructor: A user-defined type definition from the Parser
        /// </summary>
        /// <param name="t">The PType type</param>
        /// <param name="nm">The name of the new type</param>
        /// <param name="dt">The representation datatype</param>
        /// <param name="db">The local database</param>
        protected PType(Type t, string nm, long ud, Domain dt, long u, Transaction tr)
            : base(t,nm, dt, u, tr)
		{
            underdefpos = ud;
		}
        /// <summary>
        /// Constructor: A user-defined type definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PType(Reader rdr) : base(Type.PType,rdr) {}
        /// <summary>
        /// Constructor: A user-defined type definition from the buffer
        /// </summary>
        /// <param name="t">The PType type</param>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		protected PType(Type t, Reader rdr) : base(t,rdr) {}
        protected PType(PType x, Writer wr) : base(x, wr)
        {
            structdefpos = wr.Fix(x.structdefpos);
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
            wr.PutLong(structdefpos);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            structdefpos = rdr.GetLong();
            base.Deserialise(rdr);
        }
        /// <summary>
        /// A readable version of the Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString() 
		{ 
			string a = "PType "+name + " ";
            a += base.ToString();
            a += "[" + structdefpos + "]";
			return a;
		}
        public override long Conflicts(Database db, Transaction tr, Physical that)
        {
            switch(that.type)
            {
                case Type.Drop:
                        if (structdefpos == ((Drop)that).delpos)
                            return ppos;
                    break; // base class has other reasons for concern
                case Type.Change:
                    if (structdefpos == ((Change)that).affects)
                        return ppos;
                    break; // base class has other reasons for concern
            }
            return base.Conflicts(db, tr, that);
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
        public PType1(string nm, long ud, Domain dt,long u,Transaction tr)
            : base(Type.PType1, nm, ud, dt, u,tr)
        {  }
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
