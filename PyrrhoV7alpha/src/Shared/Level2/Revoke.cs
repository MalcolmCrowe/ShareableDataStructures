using Pyrrho.Common;
using Pyrrho.Level3;
using Pyrrho.Level4;

// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2024
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level2
{
	/// <summary>
	/// A Level 2 Revoke request
	/// </summary>
	internal class Revoke : Grant
	{
        /// <summary>
        /// Constructor: Revoke a privilege on an object for a grantee, from the Parser
        /// </summary>
        /// <param name="pr">The privilege</param>
        /// <param name="ob">The object</param>
        /// <param name="ge">The grantee</param>
        /// <param name="pb">The local database</param>
        public Revoke(Privilege pr, long ob, long ge, long pp, Context cx)
            : this(Type.Revoke, pr, ob, ge, pp, cx)
		{}
        /// <summary>
        /// Constructor: Revoke a privilege on an object for a grantee, from the Parser
        /// </summary>
        /// <param name="tp">The Revoke type</param>
        /// <param name="pr">The privilege</param>
        /// <param name="ob">The object</param>
        /// <param name="ge">The grantee</param>
        /// <param name="pb">The local database</param>
        protected Revoke(Type tp, Privilege pr, long ob, long ge, long pp, Context cx)
            : base(tp, pr, ob, ge, pp, cx)
		{}
        /// <summary>
        /// Constructor: Revoke a privilege on an object for a grantee, from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public Revoke(Reader rdr) : base(Type.Revoke,rdr)
		{}
        protected Revoke(Revoke x, Writer wr) : base(x, wr) { }
        protected override Physical Relocate(Writer wr)
        {
            return new Revoke(this, wr);
        }
        internal override DBObject? Install(Context cx, long p)
        {
            if (cx._Ob(obj) is not DBObject ob || ob.infos[cx.role.defpos] is not ObInfo oi)
                throw new DBException("42105");
            oi += (ObInfo.Privilege, oi.priv & ~priv);
            if (oi.priv == Privilege.NoPrivilege)
            {
                ob = ob.New(ob.defpos,ob.mem+(DBObject.Infos, ob.infos - cx.role.defpos));
                cx.Add(ob);
                ob = cx._Ob(obj) ?? throw new PEException("PE42800");
                ob.Cascade(cx,Drop.DropAction.Cascade);
            }
            else
                ob = ob.New(ob.defpos,ob.mem+(DBObject.Infos, ob.infos + (cx.role.defpos, oi)));
            cx.db += (ob, p);
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            return ob;
        }
        /// <summary>
        /// a readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
			return "Revoke "+priv.ToString()+" on "+Pos(obj)+" from "+Pos(grantee);
		}
	}
}
