using Pyrrho.Common;
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
    /// Ordering associates an ordering function with a user defined type
    /// </summary>
    internal class Ordering : Physical
    {
        /// <summary>
        /// The type being ordered
        /// </summary>
        public Domain domain;
        /// <summary>
        /// the ordering function
        /// </summary>
        public long funcdefpos;
        /// <summary>
        /// The flags specified in CREATE ORDERING
        /// </summary>
        public OrderCategory flags;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (domain!=null && !wr.cx.db.types.Contains(domain)) 
                return domain.Create(wr,tr);
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
        public Ordering(Domain tp, long fn, OrderCategory fl, long pp, Context cx)
            : base(Type.Ordering,pp,cx)
        {
            domain = tp;
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
            domain = (Domain)x.domain._Relocate(wr);
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
            wr.PutLong(wr.cx.db.types[domain].Value);
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
            domain = (Domain)rdr.context.db.objects[rdr.GetLong()];
            funcdefpos = rdr.GetLong();
            flags = (OrderCategory)rdr.GetInt();
            base.Deserialise(rdr);
        }
        public override long Conflicts(Database db, Context cx, Physical that)
        {
            switch(that.type)
            {
                case Type.Ordering:
                    return (domain == ((Ordering)that).domain) ? ppos : -1;
                case Type.Drop:
                    {
                        var t = (Drop)that;
                        return (db.types[domain] == t.delpos || funcdefpos == t.delpos) ? ppos : -1;
                    }
            }
            return base.Conflicts(db, cx, that);
        }
        /// <summary>
        /// A readable version of the Physical
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            return "Ordering for " + domain.name +
                flags + Pos(funcdefpos);
        }

        internal override void Install(Context cx, long p)
        {
            var dm = domain + (Domain.OrderFunc, (Procedure)cx.db.objects[funcdefpos])
                +(Domain.OrderCategory,flags);
            cx.db += (cx.db.types[domain].Value,dm, p);
        }
    }
}
