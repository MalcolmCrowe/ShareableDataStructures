using System;
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
    /// Ordering associates an ordering function with a user defined type
    /// </summary>
    internal class Ordering : Physical
    {
        /// <summary>
        /// The type being ordered
        /// </summary>
        public long typedefpos;
        /// <summary>
        /// the ordering function
        /// </summary>
        public long funcdefpos;
        /// <summary>
        /// The flags specified in CREATE ORDERING
        /// </summary>
        public OrderCategory flags;
        public override long Dependent(Writer wr)
        {
            if (!Committed(wr,typedefpos)) return typedefpos;
            if (!Committed(wr,funcdefpos)) return funcdefpos;
            return -1;
        }
        /// <summary>
        /// Constructor: an Ordering definition from the Parser
        /// </summary>
        /// <param name="tp">The type being ordered</param>
        /// <param name="fn">The ordering function</param>
        /// <param name="fl">The ordering flags</param>
        /// <param name="db">The local database</param>
        public Ordering(long tp, long fn, OrderCategory fl, long u, Transaction tr) 
            :base(Type.Ordering,u,tr)
        {
            typedefpos = tp;
            funcdefpos = fn;
            flags = fl;
        }
        /// <summary>
        /// Constructor: an Ordering definition from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
        public Ordering(Reader rdr)
            : base(Type.Ordering, rdr)
        { }
        protected Ordering(Ordering x, Writer wr) : base(x, wr)
        {
            typedefpos = wr.Fix(x.typedefpos);
            funcdefpos = wr.Fix(x.funcdefpos);
            flags = x.flags;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Ordering(this, wr);
        }
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
        public override void Serialise(Writer wr)
        {
            typedefpos = wr.Fix(typedefpos);
            wr.PutLong(typedefpos);
            funcdefpos = wr.Fix(funcdefpos);
            wr.PutLong(funcdefpos);
            wr.PutInt((int)flags);
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            typedefpos = rdr.GetLong();
            funcdefpos = rdr.GetLong();
            flags = (OrderCategory)rdr.GetInt();
            base.Deserialise(rdr);
        }
        public override long Conflicts(Database db, Transaction tr, Physical that)
        {
            switch(that.type)
            {
                case Type.Ordering:
                    return (typedefpos == ((Ordering)that).typedefpos) ? ppos : -1;
                case Type.Drop:
                    {
                        var t = (Drop)that;
                        return (typedefpos == t.delpos || funcdefpos == t.delpos) ? ppos : -1;
                    }
            }
            return base.Conflicts(db, tr, that);
        }
        /// <summary>
        /// A readable version of the Physical
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            return "Ordering for " + Pos(typedefpos) +
                flags + Pos(funcdefpos);
        }

        internal override Database Install(Database db, Role ro, long p)
        {
            throw new NotImplementedException();
        }
    }
}
