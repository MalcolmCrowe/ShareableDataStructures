using System;
using Pyrrho.Common;
using Pyrrho.Level1;
using Pyrrho.Level3;
using Pyrrho.Level4;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2020
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
	/// An Edit record is to request an ALTER DOMAIN
	/// </summary>
	internal class Edit : PDomain
	{
        internal long _defpos;
        public long prev;
        public override long defpos => _defpos;
        /// <summary>
        /// Constructor: an Edit request from the Parser
        /// </summary>
        /// <param name="old">The previous version of the Domain</param>
        /// <param name="nm">The (new) name</param>
        /// <param name="sd">The (new) structure definition</param>
        /// <param name="dt">The (new) Sql data type</param>
        /// <param name="pb">The local database</param>
        public Edit(Domain old, string nm, Domain dt,long pp,Context cx)
            : base(Type.Edit, nm, dt.prim, dt.prec, (byte)dt.scale, dt.charSet,
                  dt.culture.Name,dt.defaultString,dt.super,pp,cx)
        {
            _defpos = old.defpos;
            prev = old.lastChange;
        }
        /// <summary>
        /// Constructor: an Edit request from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public Edit(Reader rdr) : base(Type.Edit,rdr) {}
        protected Edit(Edit x, Writer wr) : base(x, wr)
        {
            _defpos = wr.Fix(x._defpos);
            prev = wr.Fix(x.prev);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Edit(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Reclocation info for Positions</param>
        public override void Serialise(Writer wr)
		{
            wr.PutLong(prev);
			base.Serialise(wr);
		}
        /// <summary>
        /// Deserialise from the buffer
        /// </summary>
        /// <param name="buf">The buffer</param>
        public override void Deserialise(Reader rdr)
		{
			var prev = rdr.GetLong();
            _defpos = (long)(rdr.context.db.mem[prev]??ppos);
			base.Deserialise(rdr);
		}
        /// <summary>
        /// A readable version of the Edit
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
			return "Edit ["+Pos(_defpos)+"] "+base.ToString();
		}
        /// <summary>
        /// Read Check: conflict if affected Physical is updated
        /// </summary>
        /// <param name="pos">the position</param>
        /// <returns>whether a conflict has occurred</returns>
		public override DBException ReadCheck(long pos)
		{
			return (pos==defpos)?new DBException("40009", pos).Mix() :null;
		}
        public override long Affects => _defpos;
        public override long Conflicts(Database db, Transaction tr, Physical that)
        {
            switch (that.type)
            {
                case Type.Record3:
                case Type.Record2:
                case Type.Record1:
                case Type.Record:
                case Type.Update1:
                case Type.Update:
                    {
                        var t = (Record)that;
                        for (var cp = t.fields.PositionAt(0); cp != null; cp = cp.Next())
                        {
                            var c = (DBObject)db.objects[cp.key()];
                            if (c.domain.defpos == defpos)
                                return ppos;
                        }
                        break;
                    }
                case Type.Drop:
                    return (((Drop)that).delpos == defpos) ? ppos : -1;
            }
            return base.Conflicts(db, tr, that);
        }
	}
}
