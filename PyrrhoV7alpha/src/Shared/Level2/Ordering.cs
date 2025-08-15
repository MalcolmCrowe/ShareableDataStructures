using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

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
            if (domain != null && wr.cx.db.Find(domain) is null)
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
        /// <param name="pp">The transaction's position for this Physical</param>
        /// <param name="cx">The Context</param>
        public Ordering(Domain tp, long fn, OrderCategory fl, long pp, Context cx)
            : base(Type.Ordering,pp,cx,"",Grant.AllPrivileges)
        {
            domain = tp;
            domdefpos = tp.defpos;
            funcdefpos = fn;
            flags = fl;
        }
        /// <summary>
        /// Constructor: an Ordering definition from the buffer
        /// </summary>
        /// <param name="rdr">The Reader for the file</param>
        public Ordering(Reader rdr)
            : base(Type.Ordering, rdr)
        { }
        /// <summary>
        /// Update the internal fields when serialising to the file
        /// </summary>
        /// <param name="x">The Transaction version of this Physical</param>
        /// <param name="wr">The Writer for the file</param>
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
        /// <param name="wr">The Writer for the file</param>
        public override void Serialise(Writer wr)
        {
            wr.PutLong(wr.cx.db.Find(domain)?.defpos??throw new PEException("PE48800"));
            funcdefpos = wr.cx.Fix(funcdefpos);
            wr.PutLong(funcdefpos);
            wr.PutInt((int)flags);
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="rdr">the Reader for the file</param>
        public override void Deserialise(Reader rdr)
        {
            domdefpos = rdr.GetLong();
            funcdefpos = rdr.GetLong();
            flags = (OrderCategory)rdr.GetInt();
            base.Deserialise(rdr);
            rdr.Setup(this);
        }
        /// <summary>
        /// At the validation point of the Transaction, we check this against all
        /// Physicals committed to the Database since our transaction start.
        /// Watch out for and return an exception 
        /// </summary>
        /// <param name="db">The Database</param>
        /// <param name="cx">The Context</param>
        /// <param name="that">A Physical to check</param>
        /// <param name="ct">The Physical describing the Transaction</param>
        /// <returns>An exception if reported</returns>
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
                        if (db.Find(domain)?.defpos == t.delpos || funcdefpos == t.delpos)
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
        /// <summary>
        /// Update the Database or Transaction with the new information
        /// </summary>
        /// <param name="cx">The Context</param>
        /// <returns>The updated Domain (may be a Table/RowSet/Type etc)</returns>
        internal override DBObject? Install(Context cx)
        {
            var dm = domain 
                + (Domain.OrderFunc, (Procedure?)cx.db.objects[funcdefpos] ?? throw new DBException("42108"))
                +(Domain._OrderCategory, flags);
            cx.Add(dm);
            cx.db += dm;
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            return dm;
        }
    }
}
