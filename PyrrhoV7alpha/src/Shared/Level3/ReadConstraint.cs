using System.Text;
using Pyrrho.Common;
using Pyrrho.Level2;
using Pyrrho.Level4;
// Pyrrho Database Engine by Malcolm Crowe at the University of the West of Scotland
// (c) Malcolm Crowe, University of the West of Scotland 2004-2022
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
    ///	// shareable as of 26 April 2021
    /// </summary>
    internal class ReadConstraint
    {
        public readonly long tabledefpos;
		public readonly CheckUpdate check = null;
     /*   public ReadConstraint(Context cx, long d, RowSet rs, Index index, PRow match) 
            : this(d,_Check(cx,rs,index,match))
        { } */
        internal ReadConstraint(long tb,CheckUpdate ch)
        {
            tabledefpos = tb;
            check = ch;
        }
        /*
        static CheckUpdate _Check(Context cx,RowSet rs,Index index,PRow match)
        {
            var cs = CTree<long,bool>.Empty;
            var dm = cx._Dom(rs);
            var n = (dm.display == 0) ? int.MaxValue : dm.display;
            if (cx.exec is SelectStatement)
                for (var b = dm.rowType.First(); b != null && b.key() < n; b = b.Next())
                    cs += (b.value(), true);
            var rq = index?.rows?.Get(match);
            return (rq == null) ? new CheckUpdate(cs) : new CheckSpecific(rq.Value, cs);
        } */
        public static ReadConstraint operator +(ReadConstraint rc,
            TableRowSet.TableCursor cu)
        {
            return (rc.check != null) ? new ReadConstraint(rc.tabledefpos, rc.check.Add(cu))
            : new ReadConstraint(rc.tabledefpos, // first constraint on this table
                new CheckSpecific(cu._trs.rdCols, // therefore specific
                    new CTree<long, bool>(cu._ds[rc.tabledefpos].Item1, true)));
        }
        /// <summary>
        /// Examine the consequences of changes to the object
        /// </summary>
        /// <param name="p">the change</param>
        /// <returns>whether we have a transaction conflict</returns>
		public DBException Check(Physical p,PTransaction ct)
        {
            if (check == null)
                return p.ReadCheck(tabledefpos,p,ct);
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
            return "[" + tabledefpos + " "+ (check?.ToString() ?? "") + "]";
        }
    }
    /// <summary>
    /// A clever class for checking transaction conflicts between read actions in one transaction
    /// and write/updates in another.
    /// Subclasses record other sorts of constraint: eg CheckSpecific, BlockUpdate.
    /// // shareable as of 26 April 2021
    /// </summary>
	internal class CheckUpdate
    {
        /// <summary>
        /// a list of ReadColumn
        /// </summary>
		public CTree<long,bool> rdcols = CTree<long,bool>.Empty;
        /// <summary>
        /// Constructor: a read operation involving the readConstraint's database object
        /// </summary>
        /// <param name="cx">The context</param>
        public CheckUpdate(CTree<long,bool> rt)
        {
            rdcols = rt;
        }
        public virtual CheckUpdate Add(TableRowSet.TableCursor cu)
        {
            var cs = cu._trs.rdCols;
            return (cs == rdcols)?this: new CheckUpdate(cs+rdcols);
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
            var sb = new StringBuilder(GetType().Name);
            var cm = " (";
            for (var e = rdcols?.First(); e != null; e = e.Next())
            {
                sb.Append(cm);cm = ",";
                sb.Append(DBObject.Uid(e.key()));
            }
            if (cm==",")
                sb.Append(")");
            return sb.ToString();
        }
    }
    /// <summary>
    /// A readConstraint for specific rows
    /// // shareable as of 26 April 2021
    /// </summary>
	internal class CheckSpecific : CheckUpdate
    {
        /// <summary>
        /// A list of Row (from MakeKey)
        /// </summary>
		public readonly CTree<long, bool> recs = CTree<long, bool>.Empty;
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="cx">The context</param>
        public CheckSpecific(CTree<long,bool> rs,CTree<long,bool> cs) : base(cs) 
        {
            recs = rs;
        }
        /*
        public CheckSpecific(long rp, CTree<long, bool> cs) 
            : this(new CTree<long,bool>(rp,true),cs)
        { } */
        public override CheckUpdate Add(TableRowSet.TableCursor cu)
        {
            var cs = cu._trs.rdCols;
            var (dp, pp) = cu._ds[cu._trs.target];
            return (cs.CompareTo(rdcols)!=0) ? base.Add(cu) :
                recs.Contains(dp) ? this :
                new CheckSpecific(recs + (dp, true),cs);
        }
        /// <summary>
        /// Test for conflict against a given insert/update/deletion
        /// </summary>
        /// <param name="r">the insert/update</param>
        /// <returns>whether conflict has occurred</returns>
		public override DBException Check(Record r, PTransaction ct)
        {
            if (recs.Contains(r.defpos))
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
            tableProfile.specific = (int)recs.Count;
            base.Profile(db, tableProfile);
        }
        /// <summary>
        /// Return a parsable version of this CheckUpdate
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            var sb = new StringBuilder(base.ToString());
            var cm = "[";
            for (var b=recs.First();b!=null;b=b.Next())
            {
                sb.Append(cm);cm = ",";
                sb.Append(DBObject.Uid(b.key()));
            }
            sb.Append("]");
            return sb.ToString();
        }
    }
    /*
    /// <summary>
    /// The readConstraint blocks Updates for a database object
    /// // shareable as of 26 April 2021
    /// </summary>
	internal class BlockUpdate : CheckUpdate
    {
        /// <summary>
        /// Constructor for a local database
        /// </summary>
        /// <param name="cx">The context</param>
        public BlockUpdate() : base(CTree<long,bool>.Empty) { }
        /// <summary>
        /// If we have a list of TableColumns use them.
        /// Otherwise signal transaction conflict on a change to our table
        /// </summary>
        /// <param name="r">A Record to check</param>
        /// <returns>An exception (null means no problem)</returns>
        public override DBException Check(Record r,PTransaction ct)
        {
            if (rdcols != BTree<long,bool>.Empty)
                return base.Check(r,ct);
            return new DBException("40008", r.tabledefpos,r,ct).Mix();
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
            return "-";
        } 
    } */

}
