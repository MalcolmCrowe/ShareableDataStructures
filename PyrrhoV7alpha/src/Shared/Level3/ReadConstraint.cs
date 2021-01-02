using System.Collections.Generic;
using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2021
//
// This software is without support and no liability for damage consequential to use.
// You can view and test this code, and use it subject for any purpose.
// You may incorporate any part of this code in other software if its origin 
// and authorship is suitably acknowledged.
// All other use or distribution or the construction of any product incorporating 
// this technology requires a license from the University of the West of Scotland.

namespace Pyrrho.Level3
{
    /// <summary>
    /// ReadConstraints record all of the objects that have been accessed in the current transaction
    /// so that this transaction will conflict with a transaction that changes any of them.
    /// However, for records in a table, we allow specific non-conflicting updates, as follows:
    ///	(a) (CheckUpdate) If unique selection of specific records cannot be guaranteed, then 
    ///	we should report conflict if any column read is updated by another transaction. 
    ///	(b) (CheckSpecific) If we are sure the transaction has seen a small number of records of tb, 
    ///	selected by specific values of the primary or other unique key, then 
    ///	we can limit the conflict check to updates of the selected records (if any), 
    ///	or to updates of the key TableColumns.
    ///	(c) (BlockUpdate) as (a) but it is known that case (b) cannot apply.
    /// </summary>
    internal class ReadConstraint
    {
        readonly Context cnx;
        /// <summary>
        /// The object we have just read
        /// </summary>
        public long defpos;
        /// <summary>
        /// Record what the consequences are
        /// </summary>
		public CheckUpdate check = null;
        /// <summary>
        /// Constructor: a new empty readconstraint for the table
        /// </summary>
        /// <param name="cx">the current context</param>
        /// <param name="d">the object in question</param>
		internal ReadConstraint(Context cx, long d)
        {
            cnx = cx;
            defpos = d;
        }
        /// <summary>
        /// Add a selector to the CheckUpdate
        /// </summary>
        /// <param name="c">the selector's defining position</param>
		internal void AddSelect(long c)
        {
            check.AddSelect(c);
        }
        /// <summary>
        /// Record a singleton in the CheckUpdate (only used for primary key)
        /// </summary>
        /// <param name="x">The index</param>
        /// <param name="m">The key</param>
		public void Singleton(Index x, PRow m)
        {
            check = check.Singleton(x, m);
        }
        /// <summary>
        /// This readconstraint now blocks any updates to the object
        /// </summary>
		public void Block()
        {
            check = new BlockUpdate(cnx,defpos);
        }
        /// <summary>
        /// Examine the consequences of changes to the object
        /// </summary>
        /// <param name="p">the change</param>
        /// <returns>whether we have a transaction conflict</returns>
		public DBException Check(Physical p,PTransaction ct)
        {
            if (check == null)
                return p.ReadCheck(defpos,p,ct);
            if (p is Record r)
                return check.Check(r, ct);
            return null;
        }
        /// <summary>
        /// A parsable version of the read constraint
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return "[" + defpos + (check?.ToString() ?? "") + "]";
        }
    }
    /// <summary>
    /// A clever class for checking transaction conflicts between read actions in one transaction
    /// and write/updates in another.
    /// Subclasses record other sorts of constraint: eg CheckSpecific, BlockUpdate.
    /// </summary>
	internal class CheckUpdate
    {
        internal readonly Context cnx;
        public long tabledefpos;
        /// <summary>
        /// a list of ReadColumn
        /// </summary>
		public BTree<long, bool> rdcols = null;
        /// <summary>
        /// a specific key
        /// </summary>
        public PRow rdkey = null;
        /// <summary>
        /// Constructor: a read operation involving the readConstraint's database object
        /// </summary>
        /// <param name="cx">The context</param>
        public CheckUpdate(Context cx,long tb)
        {
            cnx = cx;
            tabledefpos = tb;
        }
        /// <summary>
        /// Add a Selector to this CheckUpdate
        /// </summary>
        /// <param name="d">the selector</param>
        internal void AddSelect(long d)
        {
            rdcols +=(d, true);
        }
        /// <summary>
        /// Make the CheckUpdate specific to a single row
        /// </summary>
        /// <param name="x">the index</param>
        /// <param name="m">the key</param>
        /// <returns>the new CheckUpdate (now a CheckSpecific)</returns>
		public virtual CheckUpdate Singleton(Index x, PRow m)
        {
            return new CheckSpecific(cnx, x, m);
        }
        /// <summary>
        /// Check an insert/update/deletion against the ReadConstraint
        /// </summary>
        /// <param name="r">the TableRow</param>
        /// <returns>conflict if any of the read TableColumns are changed</returns>
		public virtual DBException Check(Record r,PTransaction ct)
        {
            for (var c = rdcols.First(); c != null; c = c.Next())
                if (r.fields[c.key()] != null)
                    return new DBException("40006", c.key(),r,ct).Mix();
            return null;
        }
        public DBException Check(Delete r,PTransaction ct)
        {
            return (tabledefpos == r.tabledefpos) ? new DBException("40006", tabledefpos,r,ct).Mix() : null;
        }
        /// <summary>
        /// Add this readConstraint to the transaction profile
        /// </summary>
        /// <param name="db">The local database</param>
        /// <param name="tableProfile">A tableProfile</param>
        internal virtual void Profile(Database db, TableProfile tableProfile)
        {
            for (var v = rdcols.First(); v != null; v = v.Next())
            {
                var tc = db.objects[v.key()] as TableColumn;
                tableProfile.read +=(tc.defpos, true);
            }
        }
        /// <summary>
        /// Return a parsable version of the CheckUpdate
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder("-");
            for (var e = rdcols?.First(); e != null; e = e.Next())
            {
                sb.Append(" ");
                sb.Append(e.key());
            }
            return sb.ToString();
        }
    }
    /// <summary>
    /// A readConstraint for specific rows
    /// </summary>
	internal class CheckSpecific : CheckUpdate
    {
        /// <summary>
        /// The Index nominating the row
        /// </summary>
		public Index index;
        /// <summary>
        /// A list of Row (from MakeKey)
        /// </summary>
		public List<PRow> recs = new List<PRow>();
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="cx">The context</param>
        public CheckSpecific(Context cx,long tb) : base(cx,tb) { }
        /// <summary>
        /// Constructor: CheckUpdate builds this on receipt of Singleton information
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="x">the Index</param>
        /// <param name="m">the key</param>
		public CheckSpecific(Context cx, Index x, PRow m) : this(cx,x.tabledefpos)
        {
            index = x;
            recs.Add(m); // recs is initially empty here
        }
        /// <summary>
        /// Add another singleton to the CheckSpecific
        /// </summary>
        /// <param name="x">the index</param>
        /// <param name="m">the new key</param>
        /// <returns>the modified CheckUpdate (maybe BlockUpdate)</returns>
		public override CheckUpdate Singleton(Index x, PRow m)
        {
            if (index != x) // the index has changed!
                return new BlockUpdate(this);
            recs.Add(m);
            return this;
        }
        /// <summary>
        /// Test for conflict against a given insert/update/deletion
        /// </summary>
        /// <param name="r">the insert/update</param>
        /// <returns>whether conflict has occurred</returns>
		public override DBException Check(Record r, PTransaction ct)
        {
            // check for insertion (or key update) conflicting with an empty query
            var m = r?.MakeKey(index.keys);
            if (m != null)
                foreach (var rr in recs)
                    if (m._CompareTo(rr) == 0)
                        return new DBException("40009", r.defpos, r, ct).Mix();
            return null;
        }
        /// <summary>
        /// Add this CheckUpdate to a transactio profile
        /// </summary>
        /// <param name="db">The local database</param>
        /// <param name="tableProfile">the profile</param>
        internal override void Profile(Database db, TableProfile tableProfile)
        {
            tableProfile.ckix = index.defpos;
            tableProfile.specific = recs.Count;
            base.Profile(db, tableProfile);
        }
        /// <summary>
        /// Return a parsable version of this CheckUpdate
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            sb.Append("-");
            foreach (var r in recs)
                sb.Append(r.ToString());
            return sb.ToString();
        }
    }
    /// <summary>
    /// The readConstraint blocks Updates for a database object
    /// </summary>
	internal class BlockUpdate : CheckUpdate
    {
        /// <summary>
        /// Constructor for a local database
        /// </summary>
        /// <param name="cx">The context</param>
        public BlockUpdate(Context cx,long tb) : base(cx,tb) { }
        /// <summary>
        /// Constructor: all updates for the specified TableColumns of this table should now be blocked
        /// </summary>
        /// <param name="cu">the checkupdate</param>
		public BlockUpdate(CheckSpecific cu) : this(cu.cnx,cu.index.tabledefpos)
        {
            rdcols = cu.rdcols;
        }
        /// <summary>
        /// No-op on singleton (updates are already blocked)
        /// </summary>
        /// <param name="x">The Index</param>
        /// <param name="m">The key</param>
        /// <returns>This BlockUpdate</returns>
		public override CheckUpdate Singleton(Index x, PRow m)
        {
            return this;
        }
        /// <summary>
        /// If we have a list of TableColumns use them.
        /// Otherwise signal transaction conflict on a change to our table
        /// </summary>
        /// <param name="r">A Record to check</param>
        /// <returns>An exception (null means no problem)</returns>
        public override DBException Check(Record r,PTransaction ct)
        {
            if (rdcols != null)
                return base.Check(r,ct);
            return (tabledefpos==r.tabledefpos)? new DBException("40008", tabledefpos,r,ct).Mix():null;
        }
        /// <summary>
        /// Add this readConstraint to the transaction profile
        /// </summary>
        /// <param name="db">the local database</param>
        /// <param name="tableProfile">A table profile</param>
        internal override void Profile(Database db, TableProfile tableProfile)
        {
            tableProfile.blocked = true;
            base.Profile(db, tableProfile);
        }
        /// <summary>
        /// A parsable version of the CheckUpdate
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return "-" + tabledefpos;
        }
    }

}
