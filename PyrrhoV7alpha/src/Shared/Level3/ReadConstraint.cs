using System;
using System.Collections.Generic;
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
    /// ReadConstraints record all of the objects that have been accessed in the current transaction
    /// so that this transaction will conflict with a transaction that changes any of them.
    /// However, for records in a table, we allow specific non-conflicting updates, as follows:
    ///	(a) (CheckUpdate) If unique selection of specific records cannot be guaranteed, then 
    ///	we should report conflict if any read ident is updated by another transaction. 
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
            check = new BlockUpdate(cnx);
        }
        /// <summary>
        /// Examine the consequences of changes to the object
        /// </summary>
        /// <param name="p">the change</param>
        /// <returns>whether we have a transaction conflict</returns>
		public DBException Check(Physical p)
        {
            if (check == null)
                return p.ReadCheck(defpos);
            if (p is Delete)
                return check.Check(((Delete)p).delRow);
            return check.Check((Record)p);
        }
#if !LOCAL
        /// <summary>
        /// ReadConstraints can be serialised to a remote transaction master
        /// </summary>
        /// <param name="pb">The Physical database</param>
        public void Serialise(PhysBase pb) //LOCKED
        {
            Domain.PutLong(pb, defpos);
            if (check != null)
                check.Serialise(pb);
            else
                pb.WriteByte(0);
        }
#endif
        /// <summary>
        /// We are a transaction master parsing a readconstraint from a remote server
        /// </summary>
        /// <param name="db">The local database</param>
        /// <param name="s">The string received</param>
        /// <returns>A readconstraint</returns>
        internal static ReadConstraint From(Context cnx,Database db, string s)
        {
            var ss = s.Split('-');
            if (ss.Length == 0)
                return null;
            return new ReadConstraint(cnx, long.Parse(ss[0]))
            {
                check = CheckUpdate.From(cnx,db, ss)
            };
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
        /// <summary>
        /// a list of ReadColumn
        /// </summary>
		public BTree<long, bool> rdcols = null;
        /// <summary>
        /// Constructor: a read operation involving the readConstraint's database object
        /// </summary>
        /// <param name="cx">The context</param>
        public CheckUpdate(Context cx)
        {
            cnx = cx;
        }
#if (!EMBEDDED)
        /// <summary>
        /// Constructor: a read operation of a local object by a remote server
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="svr">The server</param>
        public CheckUpdate(Context cx, PyrrhoServer svr) : this(cx)
        {
            int n = svr.tcp.GetInt();
            for (int j = 0; j < n; j++)
            {
                long d = svr.tcp.GetLong();
                rdcols +=(d, true);
            }
        }
#endif
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
        /// <param name="r">the Record</param>
        /// <returns>conflict if any of the read TableColumns are changed</returns>
		public virtual DBException Check(Record r)
        {
            for (var c = rdcols.First(); c != null; c = c.Next())
                if (r.fields[c.key()] != null)
                    return new DBException("40006", c.key()).Mix();
            return null;
        }
        public virtual DBException Check(TableRow r)
        {
            for (var c = rdcols.First(); c != null; c = c.Next())
                if (r.fields[c.key()] != null)
                    return new DBException("40006", c.key()).Mix();
            return null;
        }
#if !LOCAL
        /// <summary>
        /// Serialise this readconstraint to a remote transaction master
        /// </summary>
        /// <param name="pb">The Physical database</param>
        internal virtual void Serialise(PhysBase pb) //LOCKED
        {

            pb.WriteByte(0x01);
            if (rdcols == null)
                Domain.PutLong(pb, 0);
            else
            {
                Domain.PutLong(pb, rdcols.Count);
                for (var c = rdcols.First(); c != null; c = c.Next())
                    Domain.PutLong(pb, c.key());
            }
        }
#endif
        /// <summary>
        /// Add this readConstraint to the transaction profile
        /// </summary>
        /// <param name="db">The local database</param>
        /// <param name="tableProfile">A tableProfile</param>
        internal virtual void Profile(Database db, TableProfile tableProfile)
        {
            for (var v = rdcols.First(); v != null; v = v.Next())
            {
                var tc = db.role.objects[v.key()] as TableColumn;
                tableProfile.read +=(tc.defpos, true);
            }
        }
        /// <summary>
        /// Parse a checkupdate from a set of strings
        /// </summary>
        /// <param name="db">the local database</param>
        /// <param name="ss">a list of strings</param>
        /// <returns>the CheckUpdate, CheckSpecific, BlockUpdate</returns>
        internal static CheckUpdate From(Context cnx, Database db, string[] ss)
        {
            var r = new CheckUpdate(cnx);
            if (ss.Length > 1)
            {
                var ks = ss[1].Split(' ');
                foreach (var k in ks)
                {
                    var p = long.Parse(k);
                    if (db.role.objects[p] as Selector == null)
                        return null;
                    r.AddSelect(p);
                }
            }
            if (ss.Length == 2)
                return r;
            var s = ss[2];
            var ix = s.IndexOf('(');
            var rs = (ix < 0) ? s : s.Substring(0, ix - 1);
            var xd = long.Parse(rs);
            var rx = db.role.objects[xd] as Index;
            if (rx == null)
                return null;
            var dt = rx.keyType;
            var cs = s.Substring(ix).ToCharArray();
            var sc = new Scanner(cs, 0);
            r = new CheckSpecific(cnx, rx, new PRow(dt.Parse(sc)));
            while (sc.pos < cs.Length)
                r = r.Singleton(rx, new PRow(dt.Parse(sc)));
            if (ss.Length == 3)
                return r;
            var d = long.Parse(ss[3]);
            return new BlockUpdate(r as CheckSpecific);
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
        public CheckSpecific(Context cx) : base(cx) { }
        /// <summary>
        /// Constructor: CheckUpdate builds this on receipt of Singleton information
        /// </summary>
        /// <param name="cx">the context</param>
        /// <param name="x">the Index</param>
        /// <param name="m">the key</param>
		public CheckSpecific(Context cx, Index x, PRow m) : this(cx)
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
		public override DBException Check(Record r)
        {
            if (r is Update)
            {
                var c = Check(((Update)r).oldRow);
                if (c != null)
                    return c;
            }
            // check for insertion (or key update) conflicting with an empty query
            var m = r.MakeKey(index.cols);
            if (m != null)
                foreach (var rr in recs)
                    if (m._CompareTo(rr) == 0)
                        return new DBException("40005", r.defpos).Mix();
            return null;
        }
#if !LOCAL
        /// <summary>
        /// Serialise this CheckUpdate to a remote transaction master
        /// </summary>
        /// <param name="pb">The PhysBase</param>
        internal override void Serialise(PhysBase pb) //LOCKED
        {
            pb.WriteByte(0x2);
            Domain.PutLong(pb, index.defpos);
            Domain.PutInt(pb, recs.Count);
            foreach (var rr in recs)
                Domain.PutLong(pb, (long)index.rows.Get(pb.cnx as Transaction,rr));
        }
#endif
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
#if (!EMBEDDED)
    /// <summary>
    /// Low-level version of CheckSpecific for use with transaction masters
    /// </summary>
    internal class MasterSpecific : CheckUpdate
    {
        /// <summary>
        /// The defining position of the index (not the index itself!)
        /// </summary>
        public long indexDefPos;
        /// <summary>
        /// Defining positions of the records
        /// </summary>
        public long[] recs;
        /// <summary>
        /// Constructor: a CheckSpecific from a remote master
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="svr">The remote server</param>
        public MasterSpecific(Context cx, PyrrhoServer svr) : base(cx)
        {
            indexDefPos = svr.tcp.GetLong();
            int n = svr.tcp.GetInt();
            recs = new long[n];
            for (int j = 0; j < n; j++)
                recs[j] = svr.tcp.GetLong();
        }
        /// <summary>
        /// Test for conflict against a given insert/update/deletion
        /// </summary>
        /// <param name="r">the insert/update</param>
        /// <returns>whether conflict has occurred</returns>
        public override DBException Check(Record r)
        {
            if (r is Update)
            {
                var c = Check(((Update)r).oldRow);
                if (c != null)
                    return c;
            }
            // check for insertion (or key update) conflicting with an empty query
            int n = recs.Length;
            for (int j = 0; j < n; j++)
                if (r.defpos == recs[j])
                    return new DBException("40005", r.defpos).Mix();
            return null;
        }
    }
#endif
    /// <summary>
    /// The readConstraint blocks Updates for a database object
    /// </summary>
	internal class BlockUpdate : CheckUpdate
    {
        /// <summary>
        /// the table we are dealing with
        /// </summary>
        public long defpos;
        /// <summary>
        /// Constructor for a local database
        /// </summary>
        /// <param name="cx">The context</param>
        public BlockUpdate(Context cx) : base(cx) { }
        /// <summary>
        /// Constructor: all updates for the specified TableColumns of this table should now be blocked
        /// </summary>
        /// <param name="cu">the checkupdate</param>
		public BlockUpdate(CheckSpecific cu) : this(cu.cnx)
        {
            defpos = cu.index.tabledefpos;
            rdcols = cu.rdcols;
        }
#if (!EMBEDDED)
        /// <summary>
        /// Constructor: a blockUpdate from a remote server
        /// </summary>
        /// <param name="cx">The context</param>
        /// <param name="svr">The remote server</param>
        public BlockUpdate(Context cx, PyrrhoServer svr) : this(cx)
        {
            defpos = svr.tcp.GetLong();
            int rp = svr.tcp.ReadByte();
            if (rp < 0)
                throw new PEException("PE822");
            int n = svr.tcp.GetInt();
            for (int j = 0; j < n; j++)
            {
                long d = svr.tcp.GetLong();
                rdcols +=(d, true);
            }
        }
#endif
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
        public override DBException Check(Record r)
        {
            if (rdcols != null)
                return base.Check(r);
            return (defpos == r.tabledefpos) ? new DBException("40008", defpos).Mix() : null;
        }
#if !LOCAL
        /// <summary>
        /// Serialise ourselves to a remote transaction master
        /// </summary>
        /// <param name="pb">The Physical database</param>
        internal override void Serialise(PhysBase pb) //LOCKED
        {
            pb.WriteByte(0x3);
            Domain.PutLong(pb, defpos);
            base.Serialise(pb);
        }
#endif
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
            return "-" + defpos;
        }
    }
#if !LOCAL && !EMBEDDED
    /// <summary>
    /// We are a transaction master. This class is a collection of remote read constraints
    /// </summary>
    internal class ReadConstraintInfo
    {
        /// <summary>
        /// The connection
        /// </summary>
        public string conn;
        /// <summary>
        /// The database highwatermark
        /// </summary>
        public long dbpos;
        /// <summary>
        /// The list of readConstraints
        /// </summary>
        public BTree<long, ReadConstraint> rdc = BTree<long, ReadConstraint>.Empty;
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="c">The connection name</param>
        /// <param name="d">The database highwatermark</param>
        public ReadConstraintInfo(string c, long d) { conn = c; dbpos = d; }
        /// <summary>
        /// Parse a string of readconstraints from a remote server
        /// </summary>
        /// <param name="db">the local database</param>
        /// <param name="s"></param>
        /// <returns></returns>
        internal static ReadConstraintInfo From(Database db, string s)
        {
            var ix = s.IndexOf(' ');
            var c = s.Substring(1, ix - 1);
            var ss = s.Substring(ix + 1).Split('|');
            var d = long.Parse(ss[0]);
            var r = new ReadConstraintInfo(c, d);
            for (int i = 1; i < ss.Length; i++)
            {
                var p = ss[i];
                ix = p.IndexOf(' ');
                d = long.Parse(p.Substring(0, ix));
                r.rdc+=(d, ReadConstraint.From(db, p.Substring(ix + 1)));
            }
            return r;
        }
        /// <summary>
        /// Check this list of readconstraints
        /// </summary>
        /// <param name="db">the local database</param>
        /// <returns>Whether a conflict has occurred</returns>
        internal bool Check(Database db)
        {
            return db.pb.Check(this);
        }
        /// <summary>
        /// A parsable version of the ReadConstraintInfo
        /// </summary>
        /// <returns>a string</returns>
        public override string ToString()
        {
            var sb = new StringBuilder(";");
            sb.Append(conn);
            sb.Append(" ");
            sb.Append(dbpos);
            for (var a = rdc.First(); a != null; a = a.Next())
                sb.Append("|" + a.value().ToString());
            return sb.ToString();
        }
    }
#endif
}
