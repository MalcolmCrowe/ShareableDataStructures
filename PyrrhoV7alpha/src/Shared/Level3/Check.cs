using System;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
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
    /// Database Object for Check constraints
    /// Immutable
    /// </summary>
    internal class Check : DBObject
    {
        internal const long
            CheckObj = -51, // long
            Condition = -52, // SqlValue
            Source = -53; // string
        /// <summary>
        /// The object to which the check applies
        /// </summary>
        internal long checkobjpos => (long)mem[CheckObj];
        /// <summary>
        /// The source SQL for the check constraint
        /// </summary>
        internal string source => (string)mem[Source];
        internal SqlValue search => (SqlValue)mem[Condition];
        /// <summary>
        /// Constructor: from the level 2 information
        /// </summary>
        /// <param name="c">The PCheck</param>
        /// <param name="definer">The defining role</param>
        /// <param name="owner">the owner</param>
		public Check(PCheck c, Database db) 
            : base(c.name, c.ppos, c.ppos, db.role.defpos,BTree<long,object>.Empty
                  + (CheckObj,c.ckobjdefpos)+(Source,c.check)
                  + (Condition, new Parser(db).ParseSqlValue(c.check, Domain.Content)))
        { }
        /// <summary>
        /// Constructor: copy with changes
        /// </summary>
        /// <param name="c">The check</param>
        /// <param name="us">The new list of grantees (including ownership)</param>
        /// <param name="ow">the owner</param>
        protected Check(long dp, BTree<long, object> m) : base(dp, m) { }
        /// <summary>
        /// a readable version of the object
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append(" CheckObj="); sb.Append(Uid(checkobjpos));
            sb.Append(" Source="); sb.Append(source);
            return sb.ToString();
        }
        /// <summary>
        /// Used in renaming/drop transactions
        /// </summary>
        /// <param name="t">A (rename/drop) transaction</param>
        /// <returns>DROP,RESTRICT,or NO ACTION </returns>
        public override Sqlx Dependent(Transaction t,Context cx)
        {
            if (t.refObj.defpos == checkobjpos)
                return Sqlx.DROP;
            if (search != null)
            {
                for (var a = search.dependents.First(); a != null; a = a.Next())
                    if (a.value() == t.refObj.defpos)
                        return Sqlx.RESTRICT;
            }
            return Sqlx.NO;
        }
        internal override Basis New(BTree<long, object> m)
        {
            return new Check(defpos,m);
        }
    }
}
