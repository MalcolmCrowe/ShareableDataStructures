using Pyrrho.Common;
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
	/// Level 2 information for checking integrity conflicts for a Record
    /// Conflicts are (re)checked at transaction commit, and we can't start
    /// looking things up at that stage, so ensure all information is at hand
	/// </summary>
	internal class IndexConstraint 
	{
       /// <summary>
        ///  the index defining position
        /// </summary>
        public long defpos;
        /// <summary>
        /// the key TableColumns for the table
        /// </summary>
		public long[] cols;
        /// <summary>
        /// The table
        /// </summary>
		public long table;
        /// <summary>
        /// The key values for the Record
        /// </summary>
		public PRow newKey;
        /// <summary>
        /// chain to the next constraint for this record
        /// </summary>
		public IndexConstraint next;
        /// <summary>
        /// Constructor: index constraint details
        /// </summary>
        /// <param name="c">the key specification</param>
        /// <param name="t">the table</param>
        /// <param name="k">the key values</param>
        /// <param name="n">the next constraint for this record</param>
		public IndexConstraint(long d,long[] c,long t,PRow k,IndexConstraint n)
		{
            defpos = d;
			cols = c;
			table = t;
			newKey = k;
			next = n;
		}
        /// <summary>
        /// A constraint will conflict with an insert for the same key
        /// </summary>
        /// <param name="r">The Record</param>
        /// <returns>Whether a conflict has occurred</returns>
		public DBException Conflict(Record r)
		{
			if (r.tabledefpos==table && Compare(newKey,r.fields)==0)
					return new DBException("40026", newKey).Mix();
			return next?.Conflict(r);
		}
        /// <summary>
        /// rec has non-null values for all keys in k
        /// </summary>
        /// <param name="k"></param>
        /// <param name="rec"></param>
        /// <returns>-1,0,1 as usual</returns>
        int Compare(PRow k, ATree<long,  object> rec)
        {
            for (int i = 0; i < k.Length;i++ )
            {
                var v = rec[cols[i]];
                if (v == null)
                    throw new PEException("PE721");
                var c = k[i].CompareTo(v);
                if (c != 0)
                    return c;
            }
            return 0;
        }
	}
    /// <summary>
    /// Level 2 information for checking referential integrity for insertion
    /// Conflicts are (re)checked at transaction commit, and we can't start
    /// looking things up at that stage, so ensure all information is at hand
    /// </summary>
	internal class ReferenceInsertionConstraint // added in Record class
	{
        /// <summary>
        /// the defining position of the index
        /// </summary>
        public long defpos;
        /// <summary>
        /// the primary key TableColumns in the referenced table
        /// </summary>
		public long[] cols;
        /// <summary>
        /// Defpos of the referenced table
        /// </summary>
		public long reftable; 
        /// <summary>
        /// a new foreign key
        /// </summary>
		public PRow newKey; 
        /// <summary>
        /// Tne next reference insertion constraint for this record
        /// </summary>
		public ReferenceInsertionConstraint next;
        /// <summary>
        /// Constructor : a new reference insertion constraint
        /// </summary>
        /// <param name="cx">The context (for user defined types)</param>
        /// <param name="d">The referenced index</param>
        /// <param name="c">the key columns in the referenced table</param>
        /// <param name="t">the referenced table</param>
        /// <param name="k">the new key values</param>
        /// <param name="n">the next reference insertion constraint for this record</param>
		internal ReferenceInsertionConstraint(long d,long[] c,long t, PRow k,ReferenceInsertionConstraint n)
		{
            defpos = d;
			cols = c;
			reftable = t;
			newKey = k;
			next = n;
		}
/*        /// <summary>
        /// A constraint will conflict with Delete for the same key
        /// </summary>
        /// <param name="q">The Delete</param>
        /// <returns>Whether a conflict has occurred</returns>
		public DBException Conflict(Delete d)
		{
			if (d.tabledefpos==reftable && newKey!=null && d.delpos._CompareTo(newKey)==0)
				return new DBException("40014", newKey).Mix(); // this localTransaction has inserted a reference to the key deleted by q
			return next?.Conflict(d);
		}
 /*       /// <summary>
        /// A constraint will conflict with Update for the same key
        /// </summary>
        /// <param name="u">The update</param>
        /// <returns>Whether a conflict has occurred</returns>
		public DBException Conflict(Update u)
		{
			// if u changes a field of the primary key or some other index, then it will conflict with
			// a new reference to its old key value (ie if this.newKey matches u.reRow)
			if (u.tabledefpos==reftable)
			{
				var m = u.oldRow.MakeKey(cols);
				if (m!=null && m._CompareTo(newKey)==0)
					return new DBException("40027", newKey).Mix();
			}
			return next?.Conflict(u);
		} */
	}
    /// <summary>
    /// Level 2 information for checking referential integrity for deletion
    /// Conflicts are (re)checked at transaction commit, and we can't start
    /// looking things up at that stage, so ensure all information is at hand
    /// </summary>
	internal class ReferenceDeletionConstraint 
	{
        /// <summary>
        /// The defining position for the index
        /// </summary>
        public long defpos;
        /// <summary>
        /// the foreign key TableColumns in the referencing table
        /// </summary>
		public long[] cols;
        /// <summary>
        /// defpos of the referencing table
        /// </summary>
		public long refingtable;
        /// <summary>
        /// a primary key in the referenced table that has been deleted
        /// </summary>
		public PRow delKey;
        /// <summary>
        ///  chain to next constraint for this record
        /// </summary>
		public ReferenceDeletionConstraint next;
        /// <summary>
        /// Constructor: New referential integrity information for deletion
        /// </summary>
        /// <param name="cx">The context for user defined types</param>
        /// <param name="d">The referenced index</param>
        /// <param name="c">The columns of the foreign key</param>
        /// <param name="t">The referencing table</param>
        /// <param name="k">The key being deleted</param>
        /// <param name="n">The next referential constraint for this Delete</param>
		public ReferenceDeletionConstraint(long d,long[] c,long t, PRow k,ReferenceDeletionConstraint n)
		{
            defpos = d;
			cols = c;
			refingtable = t;
			delKey = k;
			next = n;
		}
        /// <summary>
        /// A constraint will conflict with insert of a matching key
        /// </summary>
        /// <param name="r">The Record</param>
        /// <returns>whether a conflict has occurred</returns>
        public DBException Conflict(Record r)
        {
            if (r.tabledefpos == refingtable && delKey._CompareTo(r.MakeKey(cols)) == 0)
                return new DBException("40075",delKey).Mix(); // this localTransaction has deleted a foreign key needed by r
            return next?.Conflict(r);
        }
        /// <summary>
        /// A constraint will conflict with an update of a matching key
        /// </summary>
        /// <param name="u">The update</param>
        /// <returns>whether a conflict has occurred</returns>
		public DBException Conflict(Update u)
		{
			if (u.tabledefpos==refingtable && delKey._CompareTo(u.MakeKey(cols))==0)
					return new DBException("40075", delKey).Mix();
			return next?.Conflict(u);
		}
	}
}
