using System;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level3;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2019
//
// This software is without support and no liability for damage consequential to use
// You can view and test this code
// All other use or distribution or the construction of any product incorporating this technology 
// requires a license from the University of the West of Scotland

namespace Pyrrho.Level3
{
	/// <summary>
	/// A database object defining a user
    /// Immutable
	/// </summary>
	internal class User : Role
	{
        internal const long
            Password = -303, // string (hidden)
            InitialRole = -304, // long
            Clearance = -305; // Level
        public string pwd => (string)mem[Password]; // if "" will be set on next authentication
        public long initialRole => (long)(mem[InitialRole]??0);
        public Level clearance => (Level)mem[Clearance]??Level.D;
        /// <summary>
        /// Constructor: a User from level 2 information
        /// </summary>
        /// <param name="pu">The PUser object</param>
		public User(PUser pu,Database db) 
            : base(pu.name,pu.ppos,BTree<long,object>.Empty)
        { }
        public User(long defpos, BTree<long, object> m) : base(defpos, m) { }
        public static User operator+(User u,(long,object)x)
        {
            return new User(u.defpos, u.mem + x);
        }
        /// <summary>
        /// a readable version of the user
        /// </summary>
        /// <returns>the string representation</returns>
        public override string ToString()
		{
            var sb = new StringBuilder(base.ToString());
            if (mem.Contains(Password))
            { sb.Append(" Password:"); sb.Append((pwd.Length==0)?"":"****"); }
            if (mem.Contains(InitialRole))
            { sb.Append(" InitialRole:"); sb.Append(Uid(initialRole)); }
            sb.Append(" Clearance:"); sb.Append(clearance);
            return sb.ToString();
		}
	}
}
