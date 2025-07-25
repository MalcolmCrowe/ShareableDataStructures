using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2025
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.

namespace Pyrrho.Level3
{
	/// <summary>
	/// A database object defining a user. 
    /// During execution/query processing there must always be a valid user. 
    /// If the user name provided is not known to the database,
    /// (and is therefore using the guest role) an ad-hoc User with a transaction uid is
    /// created for the access: if an access by such a user needs to be audited, 
    /// this User will be committed to the database first.
    /// Immutable
	/// </summary>
	internal class User : Role
	{
        internal const long
            Clearance = -305; // Level
        public static User None = new User("");
        public Level clearance => (Level)(mem[Clearance]??Level.D);
        /// <summary>
        /// Constructor: a User from level 2 information
        /// </summary>
        /// <param name="pu">The PUser object</param>
		public User(PUser pu,Database db) 
            : base(pu.name,pu.ppos,BTree<long, object>.Empty + (Definer,pu.definer)
                  +(Owner,pu.owner)+(Infos,pu.infos))
        { }
        /// <summary>
        /// An ad-hoc guest user (will be reified by Transaction constructor)
        /// </summary>
        /// <param name="n"></param>
        public User(string n):base(-1L, BTree<long, object>.Empty + (ObInfo.Name,n)) { }
        public User(long defpos, BTree<long, object> m) : base(defpos, m) { }
        internal override Basis New(BTree<long, object> m)
        {
            return new User(defpos,m);
        }
        public static User operator+(User u,(long,object)x)
        {
            var (dp, ob) = x;
            if (u.mem[dp] == ob)
                return u;
            return (User)u.New(u.mem + x);
        }
        internal override DBObject New(long dp, BTree<long, object>m)
        {
            return new User(dp, m);
        }
        /// <summary>
        /// a readable version of the user
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
		{
            var sb = new StringBuilder(base.ToString());
            sb.Append(" Clearance:"); sb.Append(clearance);
            return sb.ToString();
		}
	}
}
