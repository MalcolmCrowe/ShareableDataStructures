using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;

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
    /// Ordering associates an ordering function with a user defined type
    /// </summary>
    internal class Ordering : Defined
    {
        /// <summary>
        /// The type being ordered
        /// </summary>
        public Domain domain = Domain.Content;
        public long domdefpos = -1L;
        /// <summary>
        /// the ordering function
        /// </summary>
        public long funcdefpos = -1L;
        /// <summary>
        /// The flags specified in CREATE ORDERING
        /// </summary>
        public OrderCategory flags = OrderCategory.None;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (domain != null && !wr.cx.db.types.Contains(domain))
            {
                domdefpos = domain.Create(wr, tr);
                return domdefpos;
            }
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
            domdefpos = tp.defpos;
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
            domain = (Domain)x.domain.Relocate(wr.cx);
            domdefpos = domain.defpos;
            funcdefpos = wr.cx.Fix(x.funcdefpos);
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
            wr.PutLong(wr.cx.db.types[domain]??throw new PEException("PE48800"));
            funcdefpos = wr.cx.Fix(funcdefpos);
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
            domdefpos = rdr.GetLong();
            funcdefpos = rdr.GetLong();
            flags = (OrderCategory)rdr.GetInt();
            base.Deserialise(rdr);
            rdr.Setup(this);
        }
        public override DBException? Conflicts(Database db, Context cx, Physical that, PTransaction ct)
        {
            switch(that.type)
            {
                case Type.Ordering:
                    if (domain == ((Ordering)that).domain)
                        return new DBException("40048", funcdefpos, that, ct);
                    break;
                case Type.Drop:
                    {
                        var t = (Drop)that;
                        if (db.types[domain] == t.delpos || funcdefpos == t.delpos)
                            return new DBException("40010", funcdefpos, that, ct);
                        break;
                    }
            }
            return base.Conflicts(db, cx, that, ct);
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
        internal override DBObject? Install(Context cx, long p)
        {
            var dm = domain 
                + (Domain.OrderFunc, (Procedure?)cx.db.objects[funcdefpos] ?? throw new DBException("42108"))
                +(Domain.OrderCategory, flags);
            cx.db += ((Domain)dm.New(cx,dm.mem), p);
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            return dm;
        }
    }
}
