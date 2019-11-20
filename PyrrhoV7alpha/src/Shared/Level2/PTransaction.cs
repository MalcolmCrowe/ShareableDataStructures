using System;
using System.Text;
using System.Collections.Generic;
using Pyrrho.Common;
using Pyrrho.Level1;
using Pyrrho.Level4;
using Pyrrho.Level3;

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
	/// A Transaction record in the Physical database
	/// </summary>
	internal class PTransaction : Physical
	{
        /// <summary>
        /// The number of Physicals in the Transaction
        /// </summary>
		public int nrecs = 0;
        /// <summary>
        /// The user for the transaction, checked in Commit
        /// </summary>
		public long ptuser=-1;
        public long ptrole;
        /// <summary>
        /// The transaction time, updated in Commit
        /// </summary>
		public long pttime;
        public override long Dependent(Writer wr)
        {
            return -1;
        }
        /// <summary>
        /// Constructor: a Transaction record for a Commit
        /// </summary>
        /// <param name="nr">The number of records in this transaction</param>
        /// <param name="usr">The user performing the commit</param>
        /// <param name="rl">The role for the commit</param>
        /// <param name="pb">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        public PTransaction(int nr, long usr, long rl, Transaction tr)
			: this (Type.PTransaction,nr,usr,rl,tr)
		{}
        /// <summary>
        /// Constructor: a Transaction record for a Commit
        /// </summary>
        /// <param name="tp">The PTransaction or PTransaction2 type</param>
        /// <param name="nr">The number of records in this transaction</param>
        /// <param name="usr">The user performing the commit</param>
        /// <param name="rl">The role for the commit</param>
        /// <param name="pb">The physical database</param>
        /// <param name="curpos">The current position in the datafile</param>
        protected PTransaction(Type tp, int nr, long usr, long rl, Transaction tr)
			: base (tp,tr.uid, tr)
		{
			nrecs = nr;
			ptuser = usr;
			ptrole = rl;
			pttime = DateTime.Now.Ticks;
		}
        public PTransaction(long u,Transaction tr) : base(Type.PTransaction,u,tr)
        {
            nrecs = 0;
            ptuser = tr.user.defpos;
            ptrole = tr.role.defpos;
            pttime = DateTime.Now.Ticks;
        }
        /// <summary>
        /// Constructor: a Transaction record from the buffer
        /// </summary>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
		public PTransaction(Reader rdr) : this(Type.PTransaction,rdr)
		{}
        /// <summary>
        /// Constructor: a Transaction record from the buffer
        /// </summary>
        /// <param name="tp">The PTransaction or PTransaction2 type</param>
        /// <param name="bp">The buffer</param>
        /// <param name="pos">The defining position</param>
        protected PTransaction(Type tp, Reader rdr)
            : base(tp, rdr)
        { }
        protected PTransaction(PTransaction x, Writer wr) : base(x, wr)
        {
            nrecs = x.nrecs;
            ptrole = x.ptrole;
            ptuser = x.ptuser;
            pttime = x.pttime;
        }
        protected override Physical Relocate(Writer wr)
        {
            return new PTransaction(this, wr);
        }
        /// <summary>
        /// Commit this transaction to the PhysBase
        /// </summary>
        /// <param name="t">The starting position of the Commit</param>
        /// <param name="reloc">Relocation information for positions</param>
        /// <returns>Dummy</returns>
		public override void Commit(Writer wr,Transaction tr)
		{
			if (ptrole==-1)
				throw new DBException("28000").ISO();
			pttime = DateTime.Now.Ticks;
			base.Commit(wr,tr);
		}
        /// <summary>
        /// Serialise this Physical to the PhysBase
        /// </summary>
        /// <param name="r">Relocation information for positions</param>
		public override void Serialise(Writer wr)
		{
            wr.PutInt(nrecs);
            wr.PutLong(ptrole); // no need for Reloc - can't be local to this transaction
            wr.PutLong(ptuser);// no need for Reloc - can't be local to this transaction
            wr.PutLong(pttime);
			// no base.Serialise() for PTransaction
		}
        /// <summary>
        /// Deserialise this Physical from the buffer
        /// </summary>
        /// <param name="buf">the buffer</param>
        public override void Deserialise(Reader rdr)
		{
			nrecs = rdr.GetInt();
			ptrole = rdr.GetLong();
			ptuser = rdr.GetLong();
			pttime = rdr.GetLong();
			// no base.Deserialise() for PTransaction
		}
        /// <summary>
        /// A readable version of this Physical
        /// </summary>
        /// <returns>the string representation</returns>
		public override string ToString()
		{
			return "PTransaction for "+nrecs+" Role="+Pos(ptrole)
                +" User="+Pos(ptuser)+" Time="+new DateTime(pttime);
		}

        internal override (Database, Role) Install(Database db, Role ro,long p)
        {
            return (db,(Role)db.objects[ptrole]);
        }
    }
    /// <summary>
    /// Import Transactions record the source of the data as a URI
    /// </summary>
    internal class PImportTransaction : PTransaction
    {
        /// <summary>
        /// A URI describing the source of the imported data
        /// </summary>
        internal string uri;
        public PImportTransaction(int nr, long usr, long au, string ur, Transaction tr)
            :base(Physical.Type.PImportTransaction,nr,usr,au,tr)
        {
            uri = ur;
        }
        public PImportTransaction(Reader rdr)
            : base(Type.PImportTransaction, rdr)
        {}
        public override void Serialise(Writer wr)
        {
            wr.PutString(uri);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            uri = rdr.GetString();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            return "PImportTransaction for " + nrecs + " Role=" + Pos(ptrole) 
                + " User=" + Pos(ptuser) + " Time=" + new DateTime(pttime) + " Source="+uri;
        }
    }
    /// <summary>
    /// Note in a Transaction record a boundary where the user and role change to the definer of a trigger
    /// </summary>
    internal class TriggeredAction : Physical
    {
        internal long trigger;
        public override long Dependent(Writer wr)
        {
            if (!Committed(wr,trigger)) return trigger;
            return -1;
        }
        internal TriggeredAction(Reader rdr) : base(Type.TriggeredAction,rdr)
        { }
        internal TriggeredAction(long tg,long u,Transaction tr) 
            : base(Type.TriggeredAction,u,tr)
        {
            trigger = tg;
        }
        protected TriggeredAction(TriggeredAction x, Writer wr) : base(x, wr)
        {
            trigger = wr.Fix(x.trigger);
        }
        protected override Physical Relocate(Writer wr)
        {
            return new TriggeredAction(this, wr);
        }
        public override void Serialise(Writer wr)
        {
            wr.PutLong(trigger);
            base.Serialise(wr);
        }
        public override void Deserialise(Reader rdr)
        {
            trigger = rdr.GetLong();
            base.Deserialise(rdr);
        }
        public override string ToString()
        {
            return "TriggeredAction " + trigger;
        }

        internal override (Database, Role) Install(Database db, Role ro, long p)
        {
            var tg = (Trigger)db.objects[trigger];
            return (db,(Role)db.objects[tg.definer]);
        }
    }
    /// <summary>
    /// If there is no primary index, an audit record applies to the whole table
    /// </summary>
    internal class Audit : Physical
    {
        internal User user;
        internal long table;
        internal long[] cols;// may be length 0
        internal string[] key; // length must match cols
        internal long timestamp;
        public override long Dependent(Writer wr)
        {
            // This would be really strange: Audit is never added to Transaction.physicals
            // but anyway it does not make sense to audit uncommitted objects 
            if (_Dependent(wr) >= 0)
                throw new DBException("0000");
            return -1;
        }
        long _Dependent(Writer wr)
        {
            if (!Committed(wr,table)) return table;
            for (var i = 0; i < cols.Length; i++)
                if (!Committed(wr,cols[i])) return cols[i];
            return -1;
        }
        internal Audit(User us,long ta, long[] c, string[] k, long ts,long u, Transaction tr) 
            : base(Type.Audit,u,tr)
        {
            user = us; table = ta; 
            timestamp = ts; cols = c; key = k;
        }
        internal Audit(Reader rdr) : base(Type.Audit, rdr) { }
        public override void Deserialise(Reader rdr)
        {
            user = rdr.db.objects[rdr.GetLong()] as User;
            table = rdr.GetLong();
            timestamp = rdr.GetLong();
            var n = rdr.GetInt();
            cols = new long[n];
            key = new string[n];
            for (var i = 0; i < n; i++)
                cols[i] = rdr.GetLong();
            for (var i = 0; i < n; i++)
                key[i] = rdr.GetString();
            base.Deserialise(rdr);
        }
        public override void Serialise(Writer wr)
        {
            user = wr.db.objects[wr.Fix(user.defpos)] as User;
            table = wr.Fix(table);
            timestamp = wr.Fix(timestamp);
            wr.PutLong(user.defpos);
            wr.PutLong(table);
            wr.PutLong(timestamp);
            wr.PutInt(key.Length);
            for (var i=0;i<cols.Length;i++)
            {
                cols[i] = wr.Fix(cols[i]);
                wr.PutLong(cols[i]);
            }
            for (var i = 0; i < key.Length; i++)
                wr.PutString(key[i]);
            base.Serialise(wr);
        }
        public override string ToString()
        {
            var sb = new StringBuilder("Audit ");
            sb.Append(user);
            sb.Append(" ["); sb.Append(table); sb.Append("] ");
            sb.Append(new DateTime(timestamp));
            sb.Append(" {");
            var n = cols.Length;
            var cm = "";
            for (var i = 0; i < n; i++)
            {
                sb.Append(cm); cm = ",";
                sb.Append(cols[i]); sb.Append('=');
                sb.Append(key[i]);
            }
            sb.Append('}');
            return sb.ToString();
        }

        internal override (Database, Role) Install(Database db, Role ro, long p)
        {
            throw new NotImplementedException();
        }

        protected override Physical Relocate(Writer wr)
        {
            throw new NotImplementedException();
        }
    }

}
