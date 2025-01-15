using System.Text;
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
    /// A Level 2 user definition. User identities are obtained from the operating system and from Grants
    /// </summary>
    internal class PUser : Defined
    {
        public override long Dependent(Writer wr, Transaction tr)
        {
            return -1;
        }
        /// <summary>
        /// Constructor: A user identity from the Parser (e.g. from GRANT)
        /// </summary>
        /// <param name="nm">The name of the user (an identifier)</param>
        /// <param name="db">The local database</param>
        public PUser(string nm, long pp, Context cx)
            : this(Type.PUser, nm, pp, cx)
        {
        }
        /// <summary>
        /// Constructor: A user identity from the Parser (e.g. GRANT)
        /// </summary>
        /// <param name="tp">The PUser type</param>
        /// <param name="nm">The name of the user (an identifier)</param>
        /// <param name="pb">The local database</param>
        protected PUser(Type tp, string nm, long pp, Context cx)
            : base(tp, pp, cx,nm,Grant.AllPrivileges)
        {
        }
        /// <summary>
        /// Constructor: A Physical from the buffer
        /// </summary>
        /// <param name="bp">the buffer</param>
        /// <param name="pos">the defining position in the buffer</param>
        public PUser(Reader rdr) : base(Type.PUser, rdr) { }
        protected PUser(PUser x, Writer wr) : base(x, wr)
        {
            name = x.name;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PUser(this, wr);
        }

        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
		public override void Serialise(Writer wr)
        {
            wr.PutString(name.ToString());
            base.Serialise(wr);
        }
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
        {
            // name = rdr.GetString();  will not work here
            var nm = rdr.GetString();
            infos = new BTree<long, ObInfo>(rdr.context.role.defpos, new ObInfo(nm, Grant.AllPrivileges));
            base.Deserialise(rdr);
        }
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
        {
            return "PUser " + name;
        }
        internal override DBObject? Install(Context cx)
        {
            var ro = cx.db.role;
            var nu = new User(this, cx.db);
            // If this is the first User to be defined, 
            // it becomes the Owner of the database, 
            // and is granted User and Admin for the rowType role
            var first = true;
            for (var b = cx.db.roles.First(); first && b != null; b = b.Next())
                if (b.value() is long bp && cx.db.objects[bp] is User)
                    first = false;
            var pr = Grant.Privilege.Select;
            if (first)
                pr = pr | Grant.Privilege.UseRole | Grant.Privilege.AdminRole;
            var ui = new ObInfo(nu.name??"", pr);
            ro += (nu.defpos, ui);
            cx.db = cx.db + nu + (Database.Roles,cx.db.roles+(name,ppos))+ro;
            cx.db += (Database.Users, cx.db.users + (name, ppos));
            if (cx.db.log!=Common.BTree<long, Type>.Empty)
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            if (first)
            {
                cx.db = cx.db + (Database.Owner, nu.defpos);
                if (cx.db is Transaction tr && tr.user is not null && tr.user.name==nu.name)
                    cx.db = cx.db + (Database.User, nu) + (Database.Role, ro);
            }
            return nu;
        }
    }
    internal class Clearance : Physical
    {
        public long _user;
        public Level clearance = Level.D;
        public override long Dependent(Writer wr, Transaction tr)
        {
            if (!Committed(wr,_user)) return _user;
            return -1;
        }
        public Clearance(Reader rdr) : base(Type.Clearance, rdr)
        { }
        public Clearance(long us, Level cl, long pp)
            : base(Type.Clearance, pp)
        {
            _user = us;
            clearance = cl;
        }
        protected Clearance(Clearance x, Writer wr) : base(x, wr)
        {
            _user = wr.cx.Fix(x._user);
            clearance = x.clearance;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new Clearance(this, wr);
        }

        public override void Serialise(Writer wr)
        {
            Level.SerialiseLevel(wr, clearance);
            wr.PutLong(_user);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            clearance = Level.DeserialiseLevel(rdr);
            _user = rdr.GetLong();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Clearance " + _user);
            clearance.Append(sb);
            return sb.ToString();
        }

        internal override DBObject? Install(Context cx)
        {
            var us = cx.db.objects[_user] as User??throw new PEException("PE8200");
            us += (User.Clearance, clearance);
            cx.db += us;
            if (cx.db.mem.Contains(Database.Log))
                cx.db += (Database.Log, cx.db.log + (ppos, type));
            cx.Add(us);
            return us;
        }
    }
}
